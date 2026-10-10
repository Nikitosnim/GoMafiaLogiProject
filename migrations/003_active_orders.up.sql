-- Точка получения заказа
ALTER TABLE orders
    ADD COLUMN IF NOT EXISTS pickup_lat DECIMAL(10, 8),
    ADD COLUMN IF NOT EXISTS pickup_lon DECIMAL(11, 8);

-- Составной индекс для выбора кандидатов при автоназначении
CREATE INDEX IF NOT EXISTS idx_couriers_status_rating ON couriers(status, rating DESC);

-- Индекс для подсчета активной загрузки курьера
CREATE INDEX IF NOT EXISTS idx_orders_courier_active ON orders(courier_id, status);