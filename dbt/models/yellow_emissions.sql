CREATE OR REPLACE TABLE yellow_tripdata_co2 AS
SELECT y.*,
ROUND((y.trip_distance*e.co2_grams_per_mile)/1000,3) AS trip_co2_kgs,
y.trip_distance/(tpep_dropoff_datetime - tpep_pickup_datetime) AS avg_mph,




FROM yellow_tripdata AS y 
JOIN emissions AS e
ON e.vehicle_type='yellow_taxi';

