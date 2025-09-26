SELECT 
    g.tpep_pickup_datetime, 
    g.tpep_dropoff_datetime,
    g.passenger_count,
    g.trip_distance,
    (g.trip_distance * e.co2_grams_per_mile) / 1000.0 AS trip_co2_kgs,
    CASE 
        WHEN date_diff('second',g.lpep_pickup_datetime,g.lpep_dropoff_datetime)>0 
        THEN g.trip_distance/(date_diff('second',g.lpep_pickup_datetime,g.lpep_dropoff_datetime)/3600)
        ELSE 0.000 
        END AS avg_mph,
    HOUR(g.tpep_pickup_datetime) AS hour_of_day,
    DAYNAME(g.tpep_pickup_datetime) AS day_of_week,
    WEEKOFYEAR(g.tpep_pickup_datetime) AS week_of_year,
    MONTHNAME(g.tpep_pickup_datetime) AS month_of_year
FROM {{source('nyc_taxi_data', 'green_tripdata')}} AS g
JOIN {{source('nyc_taxi_data', 'EMISSIONS')}} AS e ON e.vehicle_type='green_taxi'