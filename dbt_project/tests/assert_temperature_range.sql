/*
    Singular dbt Data Quality Test: assert_temperature_range
    Vérifie qu'aucune température enregistrée ne sort de l'intervalle physique réaliste (-60°C à +60°C).
*/

SELECT
    fact_weather_key,
    city_name,
    observation_time,
    temperature_celsius
FROM {{ ref('fact_weather_observations') }}
WHERE temperature_celsius < -60.0
   OR temperature_celsius > 60.0
