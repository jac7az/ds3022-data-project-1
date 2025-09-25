CREATE OR REPLACE TABLE yellow_tripdata_co2 AS
SELECT y.*,
ROUND((y.trip_distance*e.co2_grams_per_mile)/1000,3) AS trip_co2_kgs,
ROUND(y.trip_distance/(EXTRACT(EPOCH FROM (y.tpep_dropoff_datetime - y.tpep_pickup_datetime)) / 3600),3) AS avg_mph
FROM yellow_tripdata AS y
JOIN emissions AS e
ON e.vehicle_type='yellow_taxi'

