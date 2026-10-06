package services

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"delivery-system/internal/models"

	"github.com/google/uuid"
)

var (
	ErrOrderNotFound       = errors.New("order not found")
	ErrOrderNotDelivered   = errors.New("cannot review an order that is not delivered yet")
	ErrReviewAlreadyExists = errors.New("review for this order already exists")
	ErrInvalidRating       = errors.New("rating must be between 1 and 5")
)

// CreateOrderReview сохраняет отзыв и обновляет рейтинг курьера в транзакции
func (s *OrderService) CreateOrderReview(ctx context.Context, orderID uuid.UUID, req models.CreateReviewRequest) (*models.Review, error) {
	if req.Rating < 1 || req.Rating > 5 {
		return nil, ErrInvalidRating
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// Проверяем статус заказа и получаем courier_id
	var courierID uuid.NullUUID
	var orderStatus string
	queryOrder := `SELECT courier_id, status FROM orders WHERE id = $1 FOR UPDATE`
	err = tx.QueryRowContext(ctx, queryOrder, orderID).Scan(&courierID, &orderStatus)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrOrderNotFound
		}
		return nil, err
	}

	if !courierID.Valid {
		return nil, errors.New("order was not assigned to any courier")
	}

	if orderStatus != "delivered" && orderStatus != "completed" {
		return nil, ErrOrderNotDelivered
	}

	// Создаем отзыв
	var review models.Review = models.Review{ID: uuid.New()}
	queryReview := `
		INSERT INTO reviews (id, order_id, courier_id, rating, comment, created_at)
		VALUES ($1, $2, $3, $4, $5, NOW())
		RETURNING id, order_id, courier_id, rating, comment, created_at
	`
	err = tx.QueryRowContext(ctx, queryReview, review.ID, orderID, courierID.UUID, req.Rating, req.Comment).
		Scan(&review.ID, &review.OrderID, &review.CourierID, &review.Rating, &review.Comment, &review.CreatedAt)
	if err != nil {

		if strings.Contains(err.Error(), "SQLSTATE 23505") || strings.Contains(err.Error(), "unique constraint") {
			return nil, fmt.Errorf("%w: %v", ErrReviewAlreadyExists, err)
		}

		return nil, fmt.Errorf("%s: %v", "failed to insert review", err)
	}

	// Пересчитываем средний рейтинг курьера
	queryRecalc := `
		UPDATE couriers
		SET 
			total_reviews = total_reviews + 1,
			rating = ROUND(((rating * total_reviews + $1) / (total_reviews + 1))::numeric, 2),
			updated_at = NOW()
		WHERE id = $2
	`
	_, err = tx.ExecContext(ctx, queryRecalc, req.Rating, courierID.UUID)
	if err != nil {
		return nil, fmt.Errorf("failed to update courier rating: %w", err)
	}

	if err = tx.Commit(); err != nil {
		return nil, err
	}

	return &review, nil
}

// GetCourierReviews возвращает список отзывов курьера
func (s *CourierService) GetCourierReviews(ctx context.Context, courierID uuid.UUID) ([]models.Review, error) {
	query := `
		SELECT id, order_id, courier_id, rating, comment, created_at
		FROM reviews
		WHERE courier_id = $1
		ORDER BY created_at DESC
	`
	rows, err := s.db.QueryContext(ctx, query, courierID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	reviews := make([]models.Review, 0)
	for rows.Next() {
		var r models.Review
		if err := rows.Scan(&r.ID, &r.OrderID, &r.CourierID, &r.Rating, &r.Comment, &r.CreatedAt); err != nil {
			return nil, err
		}
		reviews = append(reviews, r)
	}

	return reviews, nil
}
