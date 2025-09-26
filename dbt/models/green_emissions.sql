ALTER TABLE green_tripdata ADD COLUMN IF NOT EXISTS trip_co2_kgs DECIMAL(10,3);
ALTER TABLE green_tripdata ADD COLUMN IF NOT EXISTS avg_mph DECIMAL(10,3);
ALTER TABLE green_tripdata ADD COLUMN IF NOT EXISTS hour_of_day INT;
ALTER TABLE green_tripdata ADD COLUMN IF NOT EXISTS day_of_week CHAR(12);
ALTER TABLE green_tripdata ADD COLUMN IF NOT EXISTS week_of_year INT;
ALTER TABLE green_tripdata ADD COLUMN IF NOT EXISTS month_of_year CHAR(12);

UPDATE green_tripdata AS g
SET trip_co2_kgs = (g.trip_distance*e.co2_grams_per_mile)/1000,
avg_mph = CASE
        WHEN date_diff('second',g.lpep_pickup_datetime,g.lpep_dropoff_datetime)>0 
        THEN g.trip_distance/(date_diff('second',g.lpep_pickup_datetime,g.lpep_dropoff_datetime)/3600)
        ELSE 0.000 
        END,
    hour_of_day = HOUR(g.lpep_pickup_datetime),
    day_of_week = DAYNAME(g.lpep_pickup_datetime),
    week_of_year = WEEKOFYEAR(g.lpep_pickup_datetime),
    month_of_year = MONTHNAME(g.lpep_pickup_datetime)
FROM emissions AS e
WHERE e.vehicle_type='green_taxi'