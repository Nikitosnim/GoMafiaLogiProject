package models

import (
	"time"

	"github.com/google/uuid"
)

// AssignmentWeights представляет весовые коэффициенты критериев автоназначения
type AssignmentWeights struct {
	Distance float64 `json:"distance"`
	Rating   float64 `json:"rating"`
	Workload float64 `json:"workload"`
}

// CourierScore представляет оценку одного курьера-кандидата
type CourierScore struct {
	CourierID     uuid.UUID `json:"courier_id"`
	CourierName   string    `json:"courier_name"`
	Status        string    `json:"status"`
	DistanceKm    float64   `json:"distance_km"`
	DistanceScore float64   `json:"distance_score"`
	Rating        float64   `json:"rating"`
	RatingScore   float64   `json:"rating_score"`
	Workload      int       `json:"workload"`
	WorkloadScore float64   `json:"workload_score"`
	TotalScore    float64   `json:"total_score"`
}

// AutoAssignResult представляет результат автоматического назначения курьера
type AutoAssignResult struct {
	OrderID       uuid.UUID         `json:"order_id"`
	CourierID     uuid.UUID         `json:"courier_id"`
	CourierName   string            `json:"courier_name"`
	Score         CourierScore      `json:"score"`
	Reason        string            `json:"reason"`
	Weights       AssignmentWeights `json:"weights"`
	TopCandidates []CourierScore    `json:"top_candidates"`
	Timestamp     time.Time         `json:"timestamp"`
}
