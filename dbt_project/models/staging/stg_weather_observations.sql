/*
    Staging Model: stg_weather_observations
    Nettoyage, typage et déduplication des enregistrements bruts de météo.
*/

WITH raw_data AS (
    SELECT
        city_name,
        country,
        latitude,
        longitude,
        timezone,
        observation_time,
        temperature_2m,
        relative_humidity_2m,
        apparent_temperature,
        precipitation,
        rain,
        weather_code,
        surface_pressure,
        wind_speed_10m,
        wind_direction_10m,
        ingested_at,
        ROW_NUMBER() OVER (
            PARTITION BY city_name, observation_time
            ORDER BY ingested_at DESC
        ) AS row_num
    FROM {{ source('raw_sources', 'weather_observations') }}
)

SELECT
    -- Identifiant synthétique unique
    MD5(CONCAT(city_name, '_', CAST(observation_time AS TEXT))) AS observation_id,
    
    -- Dimensions de localisation
    TRIM(city_name) AS city_name,
    TRIM(country) AS country,
    CAST(latitude AS NUMERIC(8,5)) AS latitude,
    CAST(longitude AS NUMERIC(8,5)) AS longitude,
    timezone,

    -- Horodatage
    CAST(observation_time AS TIMESTAMP) AS observation_time,
    CAST(observation_time AS DATE) AS observation_date,
    EXTRACT(HOUR FROM observation_time) AS observation_hour,

    -- Métriques physiques
    CAST(temperature_2m AS NUMERIC(5,2)) AS temperature_celsius,
    CAST(apparent_temperature AS NUMERIC(5,2)) AS apparent_temperature_celsius,
    CAST(relative_humidity_2m AS NUMERIC(5,2)) AS humidity_percentage,
    CAST(precipitation AS NUMERIC(5,2)) AS precipitation_mm,
    CAST(rain AS NUMERIC(5,2)) AS rain_mm,
    CAST(weather_code AS INT) AS weather_code,
    CAST(surface_pressure AS NUMERIC(7,2)) AS pressure_hpa,
    CAST(wind_speed_10m AS NUMERIC(5,2)) AS wind_speed_kmh,
    CAST(wind_direction_10m AS INT) AS wind_direction_degrees,

    ingested_at
FROM raw_data
WHERE row_num = 1
  AND city_name IS NOT NULL
  AND observation_time IS NOT NULL
