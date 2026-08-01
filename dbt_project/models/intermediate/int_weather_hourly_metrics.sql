/*
    Intermediate Model: int_weather_hourly_metrics
    Calculs de caractéristiques, catégorisations et enrichissement des métriques horaires.
*/

WITH stg AS (
    SELECT * FROM {{ ref('stg_weather_observations') }}
)

SELECT
    observation_id,
    city_name,
    country,
    latitude,
    longitude,
    timezone,
    observation_time,
    observation_date,
    observation_hour,
    
    -- Températures
    temperature_celsius,
    apparent_temperature_celsius,
    ROUND((temperature_celsius * 9/5) + 32, 2) AS temperature_fahrenheit,
    
    -- Classification de température
    CASE
        WHEN temperature_celsius < 0 THEN 'Gel'
        WHEN temperature_celsius BETWEEN 0 AND 10 THEN 'Froid'
        WHEN temperature_celsius BETWEEN 10.01 AND 20 THEN 'Doux'
        WHEN temperature_celsius BETWEEN 20.01 AND 30 THEN 'Chaud'
        ELSE 'Très Chaud'
    END AS temperature_category,

    -- Humidité et Pression
    humidity_percentage,
    pressure_hpa,

    -- Précipitations
    precipitation_mm,
    rain_mm,
    CASE WHEN precipitation_mm > 0 THEN TRUE ELSE FALSE END AS is_precipitating,
    CASE WHEN precipitation_mm > 5.0 THEN TRUE ELSE FALSE END AS is_heavy_precipitation,

    -- Vent
    wind_speed_kmh,
    wind_direction_degrees,
    CASE
        WHEN wind_speed_kmh < 10 THEN 'Calme'
        WHEN wind_speed_kmh BETWEEN 10 AND 25 THEN 'Brise'
        WHEN wind_speed_kmh BETWEEN 25.01 AND 45 THEN 'Venté'
        ELSE 'Fort Vent / Bourrasque'
    END AS wind_category,

    -- Traduction des codes météo WMO
    weather_code,
    CASE
        WHEN weather_code = 0 THEN 'Ciel Dégagé'
        WHEN weather_code IN (1, 2, 3) THEN 'Partiellement Nuageux'
        WHEN weather_code IN (45, 48) THEN 'Brouillard'
        WHEN weather_code IN (51, 53, 55) THEN 'Bruine'
        WHEN weather_code IN (61, 63, 65) THEN 'Pluie'
        WHEN weather_code IN (71, 73, 75) THEN 'Neige'
        WHEN weather_code IN (80, 81, 82) THEN 'Averses'
        WHEN weather_code IN (95, 96, 99) THEN 'Orage'
        ELSE 'Indéterminé'
    END AS weather_condition_label,

    ingested_at
FROM stg
