CREATE OR REPLACE TABLE `taxi_de_project.tbl_analytics` AS (
SELECT 
v.vendor_name,
d.tpep_pickup_datetime,
d.tpep_dropoff_datetime,
pc.passenger_count,
t.trip_distance,
r.rate_code_name,
pl.pickup_latitude,
pl.pickup_longitude,
dl.dropoff_latitude,
dl.dropoff_longitude,
p.payment_type_name,
f.fare_amount,
f.extra,
f.mta_tax,
f.tip_amount,
f.tolls_amount,
f.improvement_surcharge,
f.total_amount
FROM `taxi_de_project.fact_table` f
JOIN `taxi_de_project.dim_datetime` d ON f.datetime_id = d.datetime_id
JOIN `taxi_de_project.dim_passenger_count` pc ON f.passenger_count_id = pc.passenger_count_id
JOIN `taxi_de_project.dim_trip_distance` t ON f.trip_distance_id = t.trip_distance_id
JOIN `taxi_de_project.dim_rate_code` r ON f.rate_code_id = r.rate_code_id
JOIN `taxi_de_project.dim_pickup_location` pl ON f.pickup_location_id = pl.pickup_location_id
JOIN `taxi_de_project.dim_dropoff_location` dl ON f.dropoff_location_id = dl.dropoff_location_id
JOIN `taxi_de_project.dim_payment_type` p ON f.payment_type_id = p.payment_type_id
JOIN `taxi_de_project.dim_vendor` v ON f.vendor_id = v.vendor_id
)