-- SQL Initialization Script for Data Warehouse (PostgreSQL)

-- Create schemas according to medallion / dbt architecture
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS intermediate;
CREATE SCHEMA IF NOT EXISTS analytics;

-- Create Raw Landing Table for Open-Meteo API Payloads
CREATE TABLE IF NOT EXISTS raw.weather_observations (
    id SERIAL PRIMARY KEY,
    city_name VARCHAR(100) NOT NULL,
    country VARCHAR(100) NOT NULL,
    latitude NUMERIC(8,5) NOT NULL,
    longitude NUMERIC(8,5) NOT NULL,
    timezone VARCHAR(50) NOT NULL,
    observation_time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    temperature_2m NUMERIC(5,2),
    relative_humidity_2m NUMERIC(5,2),
    apparent_temperature NUMERIC(5,2),
    precipitation NUMERIC(5,2),
    rain NUMERIC(5,2),
    weather_code INT,
    surface_pressure NUMERIC(7,2),
    wind_speed_10m NUMERIC(5,2),
    wind_direction_10m INT,
    ingested_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    raw_payload JSONB,
    CONSTRAINT unique_city_obs_time UNIQUE (city_name, observation_time)
);

-- Indexing for fast queries and incremental loading
CREATE INDEX IF NOT EXISTS idx_raw_weather_city ON raw.weather_observations(city_name);
CREATE INDEX IF NOT EXISTS idx_raw_weather_obs_time ON raw.weather_observations(observation_time);
