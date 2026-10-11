package services

import (
	"context"
	"database/sql"
	"delivery-system/internal/config"
	"delivery-system/internal/database"
	"delivery-system/internal/logger"
	"delivery-system/internal/models"
	"errors"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/google/uuid"
)

var (
	ErrAssignOrderNotFound    = errors.New("order not found")
	ErrAssignOrderNotEligible = errors.New("order is not in created status or already has a courier")
	ErrAssignNoPickupPoint    = errors.New("order has no pickup coordinates and default pickup point is not configured")
	ErrAssignNoCouriers       = errors.New("no available couriers found")
	errAssignCourierTaken     = errors.New("courier is no longer eligible")
)

type CourierAssignmentService struct {
	db  *database.DB
	cfg *config.AssignmentConfig
	log *logger.Logger
}

// NewCourierAssignmentService создает новый экземпляр сервиса автоназначения
func NewCourierAssignmentService(db *database.DB, cfg *config.AssignmentConfig, log *logger.Logger) *CourierAssignmentService {
	return &CourierAssignmentService{
		db:  db,
		cfg: cfg,
		log: log,
	}
}

// activeOrderStatuses — статусы заказов, учитываемые как загруженность курьера
var activeOrderStatuses = []string{
	string(models.OrderStatusAccepted),
	string(models.OrderStatusPreparing),
	string(models.OrderStatusReady),
	string(models.OrderStatusInDelivery),
}

// candidatesQuery возвращает кандидатов (available и busy с незаполненными слотами)
// с их текущей загруженностью, посчитанной по активным заказам
const candidatesQuery = `
	SELECT c.id, c.name, c.status, c.rating,
	       c.current_lat, c.current_lon,
	       COALESCE(w.active_count, 0) AS workload
	FROM couriers c
	LEFT JOIN (
		SELECT courier_id, COUNT(*) AS active_count
		FROM orders
		WHERE courier_id IS NOT NULL
		  AND status IN ($1, $2, $3, $4)
		GROUP BY courier_id
	) w ON w.courier_id = c.id
	WHERE c.status IN ('available', 'busy')
	  AND c.current_lat IS NOT NULL
	  AND c.current_lon IS NOT NULL
	  AND c.last_seen_at IS NOT NULL
	  AND c.last_seen_at >= $5
	LIMIT 200
`

// AutoAssignCourier автоматически назначает оптимального курьера на заказ
func (s *CourierAssignmentService) AutoAssignCourier(ctx context.Context, orderID uuid.UUID) (*models.AutoAssignResult, error) {
	//Точка получения заказа: координаты заказа, либо configured-по умолчанию
	pickupLat, pickupLon, err := s.resolvePickupPoint(ctx, orderID)
	if err != nil {
		return nil, err
	}

	// Ранжирование кандидатов по взвешенной оценке
	scores, err := s.RankCandidates(ctx, pickupLat, pickupLon)
	if err != nil {
		return nil, err
	}
	if len(scores) == 0 {
		s.log.WithField("order_id", orderID).Warn("Auto-assign: no eligible courier candidates found")
		return nil, ErrAssignNoCouriers
	}

	// Пытаемся назначить лучшего курьера; при гонке переходим к следующему
	for _, candidate := range scores {
		result, err := s.tryAssign(ctx, orderID, candidate, scores)
		if err == nil {
			s.logSelection(result, scores)
			return result, nil
		}
		if errors.Is(err, errAssignCourierTaken) {
			s.log.WithFields(map[string]interface{}{
				"order_id":   orderID,
				"courier_id": candidate.CourierID,
			}).Info("Auto-assign: candidate lost the race, trying next one")
			continue
		}
		return nil, err
	}

	return nil, ErrAssignNoCouriers
}

// RankCandidates ранжирует доступных курьеров по взвешенной оценке
func (s *CourierAssignmentService) RankCandidates(ctx context.Context, pickupLat, pickupLon float64) ([]models.CourierScore, error) {
	staleBefore := time.Now().Add(-time.Duration(s.cfg.LocationStaleMin) * time.Minute)

	rows, err := s.db.QueryContext(ctx, candidatesQuery,
		activeOrderStatuses[0], activeOrderStatuses[1], activeOrderStatuses[2], activeOrderStatuses[3],
		staleBefore,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to query courier candidates: %w", err)
	}
	defer rows.Close()

	weights := s.normalizedWeights()
	maxActive := s.cfg.MaxActiveOrders
	if maxActive <= 0 {
		maxActive = 1
	}

	scores := make([]models.CourierScore, 0)
	for rows.Next() {
		var (
			courierID uuid.UUID
			name      string
			status    string
			rating    float64
			lat       *float64
			lon       *float64
			workload  int
		)
		if err := rows.Scan(&courierID, &name, &status, &rating, &lat, &lon, &workload); err != nil {
			return nil, fmt.Errorf("failed to scan courier candidate: %w", err)
		}
		if lat == nil || lon == nil {
			continue
		}

		if workload >= maxActive {
			s.log.WithFields(map[string]interface{}{
				"courier_id":        courierID,
				"workload":          workload,
				"max_active_orders": maxActive,
			}).Debug("Auto-assign: courier excluded (workload limit reached)")
			continue
		}

		distanceKm := HaversineKm(*lat, *lon, pickupLat, pickupLon)
		scores = append(scores, s.scoreCourier(courierID, name, status, distanceKm, rating, workload, weights))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration error: %w", err)
	}

	sort.SliceStable(scores, func(i, j int) bool {
		if scores[i].TotalScore != scores[j].TotalScore {
			return scores[i].TotalScore > scores[j].TotalScore
		}
		return scores[i].DistanceKm < scores[j].DistanceKm
	})

	return scores, nil
}

// resolvePickupPoint определяет координаты точки получения заказа
func (s *CourierAssignmentService) resolvePickupPoint(ctx context.Context, orderID uuid.UUID) (float64, float64, error) {
	var pickupLat, pickupLon *float64
	err := s.db.QueryRowContext(ctx,
		`SELECT pickup_lat, pickup_lon FROM orders WHERE id = $1`, orderID).
		Scan(&pickupLat, &pickupLon)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, 0, ErrAssignOrderNotFound
		}
		return 0, 0, fmt.Errorf("failed to get order pickup point: %w", err)
	}

	if pickupLat != nil && pickupLon != nil {
		return *pickupLat, *pickupLon, nil
	}
	if s.cfg.HasDefaultPickup {
		return s.cfg.DefaultPickupLat, s.cfg.DefaultPickupLon, nil
	}
	return 0, 0, ErrAssignNoPickupPoint
}

// scoreCourier вычисляет взвешенную оценку кандидата
func (s *CourierAssignmentService) scoreCourier(
	courierID uuid.UUID, name, status string,
	distanceKm, rating float64, workload int,
	weights models.AssignmentWeights,
) models.CourierScore {
	maxDistanceKm := s.cfg.MaxDistanceKm
	if maxDistanceKm <= 0 {
		maxDistanceKm = 1
	}
	maxActive := s.cfg.MaxActiveOrders
	if maxActive <= 0 {
		maxActive = 1
	}

	distanceScore := clamp01(1 - distanceKm/maxDistanceKm)
	ratingScore := clamp01(rating / 5)
	workloadScore := clamp01(1 - float64(workload)/float64(maxActive))
	total := weights.Distance*distanceScore + weights.Rating*ratingScore + weights.Workload*workloadScore

	return models.CourierScore{
		CourierID:     courierID,
		CourierName:   name,
		Status:        status,
		DistanceKm:    round4(distanceKm),
		DistanceScore: round4(distanceScore),
		Rating:        rating,
		RatingScore:   round4(ratingScore),
		Workload:      workload,
		WorkloadScore: round4(workloadScore),
		TotalScore:    round4(total),
	}
}

// tryAssign пытается назначить одного кандидата в отдельной транзакции.
// При изменении состояния курьера/заказа параллельным запросом возвращает
// errAssignCourierTaken (пробуем следующего) или ошибку.
func (s *CourierAssignmentService) tryAssign(
	ctx context.Context,
	orderID uuid.UUID,
	candidate models.CourierScore,
	scores []models.CourierScore,
) (*models.AutoAssignResult, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Блокируем строку заказа и проверяем, что он еще назначаем
	var (
		orderStatus string
		existingCou uuid.NullUUID
	)
	err = tx.QueryRowContext(ctx,
		`SELECT status, courier_id FROM orders WHERE id = $1 FOR UPDATE`, orderID).
		Scan(&orderStatus, &existingCou)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrAssignOrderNotFound
		}
		return nil, fmt.Errorf("failed to lock order: %w", err)
	}
	if orderStatus != string(models.OrderStatusCreated) || existingCou.Valid {
		return nil, ErrAssignOrderNotEligible
	}

	// Блокируем строку курьера и заново проверяем его пригодность
	var courierStatus string
	err = tx.QueryRowContext(ctx,
		`SELECT status FROM couriers WHERE id = $1 FOR UPDATE`, candidate.CourierID).Scan(&courierStatus)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errAssignCourierTaken
		}
		return nil, fmt.Errorf("failed to lock courier: %w", err)
	}

	var workloadNow int
	err = tx.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM orders
		WHERE courier_id = $1 AND status IN ($2, $3, $4, $5)`,
		candidate.CourierID,
		activeOrderStatuses[0], activeOrderStatuses[1], activeOrderStatuses[2], activeOrderStatuses[3],
	).Scan(&workloadNow)
	if err != nil {
		return nil, fmt.Errorf("failed to recount courier workload: %w", err)
	}

	switch courierStatus {
	case string(models.CourierStatusAvailable):
		// подходит
	case string(models.CourierStatusBusy):
		maxActive := s.cfg.MaxActiveOrders
		if maxActive <= 0 {
			maxActive = 1
		}
		if workloadNow >= maxActive {
			return nil, errAssignCourierTaken
		}
	default:
		return nil, errAssignCourierTaken
	}

	now := time.Now()

	res, err := tx.ExecContext(ctx, `
		UPDATE orders SET courier_id = $1, status = $2, updated_at = $3
		WHERE id = $4 AND status = $5 AND courier_id IS NULL`,
		candidate.CourierID, models.OrderStatusAccepted, now, orderID, models.OrderStatusCreated)
	if err != nil {
		return nil, fmt.Errorf("failed to assign courier to order: %w", err)
	}
	if rowsAffected, err := res.RowsAffected(); err != nil || rowsAffected == 0 {
		return nil, ErrAssignOrderNotEligible
	}

	// Свободный курьер становится занятым; busy уже занят своими заказами
	if courierStatus == string(models.CourierStatusAvailable) {
		if _, err := tx.ExecContext(ctx,
			`UPDATE couriers SET status = $1, updated_at = $2 WHERE id = $3`,
			models.CourierStatusBusy, now, candidate.CourierID); err != nil {
			return nil, fmt.Errorf("failed to set courier busy: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit assignment: %w", err)
	}

	weights := s.normalizedWeights()
	return &models.AutoAssignResult{
		OrderID:       orderID,
		CourierID:     candidate.CourierID,
		CourierName:   candidate.CourierName,
		Score:         candidate,
		Reason:        s.buildReason(candidate),
		Weights:       weights,
		TopCandidates: topScores(scores, 5),
		Timestamp:     now,
	}, nil
}

// logSelection пишет структурированный лог выбора для прозрачности решения
func (s *CourierAssignmentService) logSelection(result *models.AutoAssignResult, scores []models.CourierScore) {
	s.log.WithFields(map[string]interface{}{
		"order_id":             result.OrderID,
		"courier_id":           result.CourierID,
		"courier_name":         result.CourierName,
		"total_score":          result.Score.TotalScore,
		"distance_km":          result.Score.DistanceKm,
		"distance_score":       result.Score.DistanceScore,
		"rating_score":         result.Score.RatingScore,
		"workload":             result.Score.Workload,
		"workload_score":       result.Score.WorkloadScore,
		"candidates_evaluated": len(scores),
		"reason":               result.Reason,
	}).Info("Auto-assign: optimal courier selected")

	for i, score := range scores {
		s.log.WithFields(map[string]interface{}{
			"order_id":     result.OrderID,
			"rank":         i + 1,
			"courier_id":   score.CourierID,
			"courier_name": score.CourierName,
			"total_score":  score.TotalScore,
			"distance_km":  score.DistanceKm,
			"rating":       score.Rating,
			"workload":     score.Workload,
		}).Debug("Auto-assign: candidate ranking")
	}
}

// buildReason формирует человекочитаемое объяснение выбора курьера
func (s *CourierAssignmentService) buildReason(score models.CourierScore) string {
	weights := s.normalizedWeights()
	return fmt.Sprintf(
		"best score %.3f: distance %.2f km (score %.2f, weight %.0f%%), rating %.2f/5 (score %.2f, weight %.0f%%), workload %d/%d (score %.2f, weight %.0f%%)",
		score.TotalScore,
		score.DistanceKm, score.DistanceScore, weights.Distance*100,
		score.Rating, score.RatingScore, weights.Rating*100,
		score.Workload, s.cfg.MaxActiveOrders, score.WorkloadScore, weights.Workload*100,
	)
}

// normalizedWeights приводит весовые коэффициенты к единичной сумме
func (s *CourierAssignmentService) normalizedWeights() models.AssignmentWeights {
	sum := s.cfg.DistanceWeight + s.cfg.RatingWeight + s.cfg.WorkloadWeight
	if sum <= 0 {
		return models.AssignmentWeights{Distance: 0.4, Rating: 0.3, Workload: 0.3}
	}
	return models.AssignmentWeights{
		Distance: s.cfg.DistanceWeight / sum,
		Rating:   s.cfg.RatingWeight / sum,
		Workload: s.cfg.WorkloadWeight / sum,
	}
}

// HaversineKm считает расстояние по great-circle формуле, в километрах
func HaversineKm(lat1, lon1, lat2, lon2 float64) float64 {
	const earthRadiusKm = 6371.0
	toRad := math.Pi / 180
	dLat := (lat2 - lat1) * toRad
	dLon := (lon2 - lon1) * toRad
	a := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(lat1*toRad)*math.Cos(lat2*toRad)*math.Sin(dLon/2)*math.Sin(dLon/2)
	return earthRadiusKm * 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
}

func clamp01(v float64) float64 {
	if v < 0 {
		return 0
	}
	if v > 1 {
		return 1
	}
	return v
}

func round4(v float64) float64 {
	return math.Round(v*10000) / 10000
}

func topScores(scores []models.CourierScore, n int) []models.CourierScore {
	if len(scores) <= n {
		return scores
	}
	return scores[:n]
}
