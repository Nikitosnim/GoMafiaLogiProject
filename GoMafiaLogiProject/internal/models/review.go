package models

import (
	"time"

	"github.com/google/uuid"
)

type Review struct {
	ID        int       `json:"id" db:"id"`
	OrderID   uuid.UUID `json:"order_id" db:"order_id"`
	CourierID uuid.UUID `json:"courier_id" db:"courier_id"`
	Rating    int       `json:"rating" db:"rating"`
	Comment   string    `json:"comment,omitempty" db:"comment"`
	CreatedAt time.Time `json:"created_at" db:"created_at"`
}

type CreateReviewRequest struct {
	Rating  int    `json:"rating"`
	Comment string `json:"comment"`
}
