package handlers

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"delivery-system/internal/kafka"
	"delivery-system/internal/logger"
	"delivery-system/internal/models"
	"delivery-system/internal/redis"
	"delivery-system/internal/services"

	"github.com/google/uuid"
)

// OrderHandler представляет обработчик заказов
type OrderHandler struct {
	orderService      *services.OrderService
	assignmentService *services.CourierAssignmentService
	producer          *kafka.Producer
	redisClient       *redis.Client
	log               *logger.Logger
}

// NewOrderHandler создает новый обработчик заказов
func NewOrderHandler(orderService *services.OrderService, assignmentService *services.CourierAssignmentService, producer *kafka.Producer, redisClient *redis.Client, log *logger.Logger) *OrderHandler {
	return &OrderHandler{
		orderService:      orderService,
		assignmentService: assignmentService,
		producer:          producer,
		redisClient:       redisClient,
		log:               log,
	}
}

// CreateOrder создает новый заказ
func (h *OrderHandler) CreateOrder(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErrorResponse(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	var req models.CreateOrderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErrorResponse(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	// Валидация запроса
	if err := h.validateCreateOrderRequest(&req); err != nil {
		writeErrorResponse(w, http.StatusBadRequest, err.Error())
		return
	}

	// Создание заказа
	order, err := h.orderService.CreateOrder(&req)
	if err != nil {
		h.log.WithError(err).Error("Failed to create order")
		writeErrorResponse(w, http.StatusInternalServerError, "Failed to create order")
		return
	}

	// Публикация события в Kafka
	if err := h.producer.PublishOrderCreated(order); err != nil {
		h.log.WithError(err).Error("Failed to publish order created event")
		// Не возвращаем ошибку клиенту, так как заказ уже создан
	}

	var assignment *models.AutoAssignResult
	var assignmentErr error
	if req.AutoAssign {
		assignment, assignmentErr = h.assignmentService.AutoAssignCourier(r.Context(), order.ID)
		if assignmentErr != nil {
			h.log.WithError(assignmentErr).Warn("Auto-assign failed on order creation")
		} else {
			courierID := assignment.CourierID
			order.Status = models.OrderStatusAccepted
			order.CourierID = &courierID

			if err := h.producer.PublishCourierAssigned(order.ID, assignment.CourierID); err != nil {
				h.log.WithError(err).Error("Failed to publish courier assigned event")
			}
			if err := h.producer.PublishOrderStatusChanged(order.ID, models.OrderStatusCreated, models.OrderStatusAccepted, &courierID); err != nil {
				h.log.WithError(err).Error("Failed to publish order status changed event")
			}

			// Инвалидация кеша курьера (его статус изменился)
			courierCacheKey := redis.GenerateKey(redis.KeyPrefixCourier, assignment.CourierID.String())
			if err := h.redisClient.Delete(r.Context(), courierCacheKey); err != nil {
				h.log.WithError(err).Error("Failed to invalidate courier cache")
			}
		}
	}

	// Кеширование заказа в Redis
	cacheKey := redis.GenerateKey(redis.KeyPrefixOrder, order.ID.String())
	if err := h.redisClient.Set(r.Context(), cacheKey, order, defaultCacheTTL); err != nil {
		h.log.WithError(err).Error("Failed to cache order")
		// Не возвращаем ошибку клиенту
	}

	h.log.WithField("order_id", order.ID).Info("Order created successfully")

	// При запросе автоназначения возвращаем расширенный ответ с решением
	if req.AutoAssign {
		response := map[string]interface{}{"order": order}
		if assignment != nil {
			response["auto_assign"] = assignment
		} else if assignmentErr != nil {
			response["auto_assign_error"] = assignmentErr.Error()
		}
		writeJSONResponse(w, http.StatusCreated, response)
		return
	}

	writeJSONResponse(w, http.StatusCreated, order)
}

// GetOrder получает заказ по ID
func (h *OrderHandler) GetOrder(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeErrorResponse(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	orderID, err := extractUUIDFromPath(r.URL.Path, "/api/orders/")
	if err != nil {
		writeErrorResponse(w, http.StatusBadRequest, "Invalid order ID")
		return
	}

	// Попытка получить из кеша
	cacheKey := redis.GenerateKey(redis.KeyPrefixOrder, orderID.String())
	var order models.Order
	if err := h.redisClient.Get(r.Context(), cacheKey, &order); err == nil {
		h.log.WithField("order_id", orderID).Debug("Order retrieved from cache")
		writeJSONResponse(w, http.StatusOK, &order)
		return
	}

	// Получение из базы данных
	orderPtr, err := h.orderService.GetOrder(orderID)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			writeErrorResponse(w, http.StatusNotFound, "Order not found")
		} else {
			h.log.WithError(err).Error("Failed to get order")
			writeErrorResponse(w, http.StatusInternalServerError, "Failed to get order")
		}
		return
	}

	// Кеширование заказа
	if err := h.redisClient.Set(r.Context(), cacheKey, orderPtr, defaultCacheTTL); err != nil {
		h.log.WithError(err).Error("Failed to cache order")
	}

	writeJSONResponse(w, http.StatusOK, orderPtr)
}

// UpdateOrderStatus обновляет статус заказа
func (h *OrderHandler) UpdateOrderStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		writeErrorResponse(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	orderID, err := extractUUIDFromPath(r.URL.Path, "/api/orders/")
	if err != nil {
		writeErrorResponse(w, http.StatusBadRequest, "Invalid order ID")
		return
	}

	var req models.UpdateOrderStatusRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErrorResponse(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	// Получение текущего заказа для определения старого статуса
	currentOrder, err := h.orderService.GetOrder(orderID)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			writeErrorResponse(w, http.StatusNotFound, "Order not found")
		} else {
			writeErrorResponse(w, http.StatusInternalServerError, "Failed to get order")
		}
		return
	}

	oldStatus := currentOrder.Status

	// Обновление статуса
	if err := h.orderService.UpdateOrderStatus(orderID, &req); err != nil {
		if strings.Contains(err.Error(), "not found") {
			writeErrorResponse(w, http.StatusNotFound, "Order not found")
		} else {
			h.log.WithError(err).Error("Failed to update order status")
			writeErrorResponse(w, http.StatusInternalServerError, "Failed to update order status")
		}
		return
	}

	// Публикация события изменения статуса
	if err := h.producer.PublishOrderStatusChanged(orderID, oldStatus, req.Status, req.CourierID); err != nil {
		h.log.WithError(err).Error("Failed to publish order status changed event")
	}

	// Инвалидация кеша
	cacheKey := redis.GenerateKey(redis.KeyPrefixOrder, orderID.String())
	if err := h.redisClient.Delete(r.Context(), cacheKey); err != nil {
		h.log.WithError(err).Error("Failed to invalidate order cache")
	}

	h.log.WithField("order_id", orderID).WithField("new_status", req.Status).Info("Order status updated")
	writeJSONResponse(w, http.StatusOK, map[string]string{"message": "Order status updated successfully"})
}

// AutoAssignCourier автоматически назначает оптимального курьера на заказ
func (h *OrderHandler) AutoAssignCourier(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErrorResponse(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	orderID, err := extractUUIDFromPath(r.URL.Path, "/api/orders/")
	if err != nil {
		writeErrorResponse(w, http.StatusBadRequest, "Invalid order ID")
		return
	}

	result, err := h.assignmentService.AutoAssignCourier(r.Context(), orderID)
	if err != nil {
		switch {
		case errors.Is(err, services.ErrAssignOrderNotFound):
			writeErrorResponse(w, http.StatusNotFound, "Order not found")
		case errors.Is(err, services.ErrAssignOrderNotEligible):
			writeErrorResponse(w, http.StatusConflict, "Order is not in created status or already has a courier")
		case errors.Is(err, services.ErrAssignNoPickupPoint):
			writeErrorResponse(w, http.StatusBadRequest, "Order has no pickup coordinates and default pickup point is not configured")
		case errors.Is(err, services.ErrAssignNoCouriers):
			writeErrorResponse(w, http.StatusConflict, "No available couriers found")
		default:
			h.log.WithError(err).Error("Failed to auto-assign courier")
			writeErrorResponse(w, http.StatusInternalServerError, "Failed to auto-assign courier")
		}
		return
	}

	// Публикация событий в Kafka
	if err := h.producer.PublishCourierAssigned(orderID, result.CourierID); err != nil {
		h.log.WithError(err).Error("Failed to publish courier assigned event")
	}
	courierID := result.CourierID
	if err := h.producer.PublishOrderStatusChanged(orderID, models.OrderStatusCreated, models.OrderStatusAccepted, &courierID); err != nil {
		h.log.WithError(err).Error("Failed to publish order status changed event")
	}

	// Инвалидация кешей заказа и курьера
	orderCacheKey := redis.GenerateKey(redis.KeyPrefixOrder, orderID.String())
	if err := h.redisClient.Delete(r.Context(), orderCacheKey); err != nil {
		h.log.WithError(err).Error("Failed to invalidate order cache")
	}
	courierCacheKey := redis.GenerateKey(redis.KeyPrefixCourier, result.CourierID.String())
	if err := h.redisClient.Delete(r.Context(), courierCacheKey); err != nil {
		h.log.WithError(err).Error("Failed to invalidate courier cache")
	}

	h.log.WithField("order_id", orderID).
		WithField("courier_id", result.CourierID).
		WithField("reason", result.Reason).
		Info("Courier auto-assigned")

	writeJSONResponse(w, http.StatusOK, result)
}

// GetOrders получает список заказов с фильтрацией
func (h *OrderHandler) GetOrders(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeErrorResponse(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	query := r.URL.Query()

	// Парсинг параметров фильтрации
	var status *models.OrderStatus
	if statusStr := query.Get("status"); statusStr != "" {
		s := models.OrderStatus(statusStr)
		status = &s
	}

	var courierID *uuid.UUID
	if courierIDStr := query.Get("courier_id"); courierIDStr != "" {
		id, err := uuid.Parse(courierIDStr)
		if err != nil {
			writeErrorResponse(w, http.StatusBadRequest, "Invalid courier ID")
			return
		}
		courierID = &id
	}

	limit := 50 // По умолчанию
	if limitStr := query.Get("limit"); limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 && l <= 100 {
			limit = l
		}
	}

	offset := 0
	if offsetStr := query.Get("offset"); offsetStr != "" {
		if o, err := strconv.Atoi(offsetStr); err == nil && o >= 0 {
			offset = o
		}
	}

	orders, err := h.orderService.GetOrders(status, courierID, limit, offset)
	if err != nil {
		h.log.WithError(err).Error("Failed to get orders")
		writeErrorResponse(w, http.StatusInternalServerError, "Failed to get orders")
		return
	}

	writeJSONResponse(w, http.StatusOK, orders)
}

// PostOrderReview создает отзыв для выполненного заказа
func (h *OrderHandler) PostOrderReview(w http.ResponseWriter, r *http.Request) {
	// Проверка метода запроса
	if r.Method != http.MethodPost {
		writeErrorResponse(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	orderID, err := extractUUIDFromPath(r.URL.Path, "/api/orders/")
	if err != nil {
		writeErrorResponse(w, http.StatusBadRequest, "Invalid order ID")
		return
	}

	var req models.CreateReviewRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErrorResponse(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	review, err := h.orderService.CreateOrderReview(r.Context(), orderID, req)
	if err != nil {
		h.log.WithError(err).Error("Failed to create order review")

		if strings.Contains(err.Error(), "not found") {
			writeErrorResponse(w, http.StatusNotFound, "Order not found")
		} else if strings.Contains(err.Error(), "rating must be between 1 and 5") {
			writeErrorResponse(w, http.StatusBadRequest, "Invalid rating")
		} else if strings.Contains(err.Error(), "review for this order already exists") {
			writeErrorResponse(w, http.StatusConflict, "Review for this order already exists")
		} else if strings.Contains(err.Error(), "not delivered") {
			writeErrorResponse(w, http.StatusConflict, "Order is not delivered yet")
		} else {
			writeErrorResponse(w, http.StatusInternalServerError, "Failed to create order review")
		}
		return
	}

	// сброс кеша
	if review != nil && review.CourierID != uuid.Nil {
		courierCaheKey := redis.GenerateKey(redis.KeyPrefixCourier, review.CourierID.String())
		err := h.redisClient.Delete(r.Context(), courierCaheKey)
		if err != nil {
			h.log.WithError(err).WithField("courier_id", review.CourierID).Error("Failed to invalidate courier cache")
		}
	}

	h.log.WithField("order_id", orderID).Info("Order review created successfully")

	writeJSONResponse(w, http.StatusCreated, review)
}

// validateCreateOrderRequest валидирует запрос на создание заказа
func (h *OrderHandler) validateCreateOrderRequest(req *models.CreateOrderRequest) error {
	if req.CustomerName == "" {
		return fmt.Errorf("customer name is required")
	}
	if req.CustomerPhone == "" {
		return fmt.Errorf("customer phone is required")
	}
	if req.DeliveryAddress == "" {
		return fmt.Errorf("delivery address is required")
	}
	if len(req.Items) == 0 {
		return fmt.Errorf("order items are required")
	}

	// Координаты точки получения: либо обе, либо ничего; валидные диапазоны
	if (req.PickupLat == nil) != (req.PickupLon == nil) {
		return fmt.Errorf("pickup_lat and pickup_lon must be provided together")
	}
	if req.PickupLat != nil && (*req.PickupLat < -90 || *req.PickupLat > 90) {
		return fmt.Errorf("pickup_lat must be between -90 and 90")
	}
	if req.PickupLon != nil && (*req.PickupLon < -180 || *req.PickupLon > 180) {
		return fmt.Errorf("pickup_lon must be between -180 and 180")
	}
	
	for i, item := range req.Items {
		if item.Name == "" {
			return fmt.Errorf("item %d: name is required", i+1)
		}
		if item.Quantity <= 0 {
			return fmt.Errorf("item %d: quantity must be positive", i+1)
		}
		if item.Price < 0 {
			return fmt.Errorf("item %d: price cannot be negative", i+1)
		}
	}

	return nil
}
