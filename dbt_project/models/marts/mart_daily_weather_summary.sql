/*
    Data Mart: mart_daily_weather_summary
    Agrégations quotidiennes des métriques météo par ville pour l'analyse décisionnelle et le reporting.
*/

WITH facts AS (
    SELECT * FROM {{ ref('fact_weather_observations') }}
)

SELECT
    MD5(CONCAT(city_name, '_', CAST(observation_date AS TEXT))) AS daily_summary_key,
    city_key,
    city_name,
    observation_date,
    
    -- Statistiques de température
    ROUND(MIN(temperature_celsius), 2) AS min_temperature_celsius,
    ROUND(MAX(temperature_celsius), 2) AS max_temperature_celsius,
    ROUND(AVG(temperature_celsius), 2) AS avg_temperature_celsius,
    ROUND(MAX(temperature_celsius) - MIN(temperature_celsius), 2) AS thermal_amplitude_celsius,
    ROUND(AVG(apparent_temperature_celsius), 2) AS avg_apparent_temperature_celsius,

    -- Statistiques d'humidité et de pression
    ROUND(AVG(humidity_percentage), 2) AS avg_humidity_percentage,
    ROUND(AVG(pressure_hpa), 2) AS avg_pressure_hpa,

    -- Statistiques de précipitations
    ROUND(SUM(precipitation_mm), 2) AS total_precipitation_mm,
    COUNT(CASE WHEN is_precipitating THEN 1 END) AS precip_hours_count,

    -- Vent
    ROUND(MAX(wind_speed_kmh), 2) AS max_wind_speed_kmh,
    ROUND(AVG(wind_speed_kmh), 2) AS avg_wind_speed_kmh,

    -- Décompte d'observations
    COUNT(*) AS total_hourly_records
FROM facts
GROUP BY 1, 2, 3, 4
