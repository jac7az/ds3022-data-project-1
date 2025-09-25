SELECT y.trip_distance,
(y.trip_distance*e.co2_grams_per_mile)/1000 AS trip_co2_kgs
FROM yellow_tripdata as y 
JOIN emissions as e
ON e.vehicle_type='yellow_taxi'