/*
    Fact Table: fact_weather_observations
    Table de faits horodatée au niveau de l'observation horaire par ville.
*/

WITH metrics AS (
    SELECT * FROM {{ ref('int_weather_hourly_metrics') }}
),
cities AS (
    SELECT * FROM {{ ref('dim_cities') }}
)

SELECT
    m.observation_id AS fact_weather_key,
    c.city_key,
    m.city_name,
    m.observation_time,
    m.observation_date,
    m.observation_hour,
    
    -- Mesures physiques
    m.temperature_celsius,
    m.apparent_temperature_celsius,
    m.temperature_fahrenheit,
    m.temperature_category,
    m.humidity_percentage,
    m.pressure_hpa,
    m.precipitation_mm,
    m.rain_mm,
    m.is_precipitating,
    m.is_heavy_precipitation,
    m.wind_speed_kmh,
    m.wind_direction_degrees,
    m.wind_category,
    m.weather_code,
    m.weather_condition_label,

    m.ingested_at
FROM metrics m
LEFT JOIN cities c ON LOWER(TRIM(m.city_name)) = LOWER(TRIM(c.city_name))
