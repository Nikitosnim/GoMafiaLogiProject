DROP TABLE IF EXISTS reviews;

DROP INDEX IF EXISTS idx_couriers_rating;

ALTER TABLE couriers
    DROP COLUMN IF EXISTS rating,
    DROP COLUMN IF EXISTS total_reviews;