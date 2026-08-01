/*
    Dimension Table: dim_cities
    Table de dimension décrivant les caractéristiques des villes surveillées.
*/

WITH metrics AS (
    SELECT * FROM {{ ref('int_weather_hourly_metrics') }}
)

SELECT
    MD5(LOWER(TRIM(city_name))) AS city_key,
    city_name,
    country,
    latitude,
    longitude,
    timezone,
    COUNT(DISTINCT observation_date) AS total_days_tracked,
    MIN(observation_time) AS first_observation_at,
    MAX(observation_time) AS latest_observation_at
FROM metrics
GROUP BY 1, 2, 3, 4, 5, 6
