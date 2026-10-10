DROP INDEX IF EXISTS idx_orders_courier_active;
DROP INDEX IF EXISTS idx_couriers_status_rating;
ALTER TABLE orders DROP COLUMN IF EXISTS pickup_lon, DROP COLUMN IF EXISTS pickup_lat;