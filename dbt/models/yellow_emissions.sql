ALTER TABLE yellow_tripdata ADD COLUMN IF NOT EXISTS trip_co2_kgs DECIMAL(10,3);
ALTER TABLE yellow_tripdata ADD COLUMN IF NOT EXISTS avg_mph DECIMAL(10,3);
ALTER TABLE yellow_tripdata ADD COLUMN IF NOT EXISTS hour_of_day INT;
ALTER TABLE yellow_tripdata ADD COLUMN IF NOT EXISTS day_of_week CHAR(12);
ALTER TABLE yellow_tripdata ADD COLUMN IF NOT EXISTS week_of_year INT;
ALTER TABLE yellow_tripdata ADD COLUMN IF NOT EXISTS month_of_year CHAR(12);

UPDATE yellow_tripdata AS y
SET trip_co2_kgs = (y.trip_distance*e.co2_grams_per_mile)/1000,
avg_mph = y.trip_distance/(DATE_DIFF('second',tpep_pickup_datetime, tpep_dropoff_datetime)/3600)
FROM emissions AS e
WHERE e.vehicle_type='yellow_taxi'