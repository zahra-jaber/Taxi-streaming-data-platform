CREATE KEYSPACE IF NOT EXISTS taxi_streaming
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};

USE taxi_streaming;

CREATE TABLE IF NOT EXISTS rides_by_id (
    ride_id text PRIMARY KEY,
    driver_id text,
    customer_id text,
    city text,
    vehicle_type text,
    distance_km double,
    fare_mad double,
    status text,
    event_timestamp timestamp
);

CREATE TABLE IF NOT EXISTS gold_city_metrics (
    city text,
    event_date date,
    window_start timestamp,
    window_end timestamp,
    total_rides int,
    total_revenue double,
    average_fare double,
    PRIMARY KEY ((city, event_date), window_start)
);