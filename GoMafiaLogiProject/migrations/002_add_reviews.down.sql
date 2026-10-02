DROP TABLE IF EXISTS reviews;

DROP INDEX IF idx_couriers_rating;

ALTER TABLE couriers
    DROP COLUMN IF EXISTS rating,
    DROP COLUMN IF EXISTS total_reviews;l