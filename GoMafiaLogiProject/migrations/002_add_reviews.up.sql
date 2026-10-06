ALTER TABLE couriers 
    ADD COLUMN IF NOT EXISTS rating NUMERIC(3, 2) DEFAULT 0.00 NOT NULL,
    ADD COLUMN IF NOT EXISTS total_reviews INT DEFAULT 0 NOT NULL;

-- Индекс для быстрой фильтрации по рейтингу
CREATE INDEX IF NOT EXISTS idx_couriers_rating ON couriers(rating DESC);

-- Таблица отзывов
CREATE TABLE IF NOT EXISTS reviews (
    id UUID PRIMARY KEY,
    order_id UUID NOT NULL UNIQUE REFERENCES orders(id) ON DELETE CASCADE,
    courier_id UUID NOT NULL REFERENCES couriers(id) ON DELETE CASCADE,
    rating INT NOT NULL CHECK (rating >= 1 AND rating <= 5),
    comment TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_reviews_courier_id ON reviews(courier_id);
