"""
PostgreSQL Database Helper Utilities for ETL Pipeline.
"""

import json
import logging
import os
from typing import Any, Dict, List
import pandas as pd
from sqlalchemy import create_engine, text

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def get_db_engine():
    """Build and return SQLAlchemy engine using environment variables."""
    user = os.getenv("POSTGRES_USER", "airflow")
    password = os.getenv("POSTGRES_PASSWORD", "airflow")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    dbname = os.getenv("POSTGRES_DB", "weather_dwh")

    connection_string = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
    return create_engine(connection_string, pool_pre_ping=True)


def initialize_database():
    """Ensure schemas and raw tables exist in PostgreSQL."""
    engine = get_db_engine()
    init_sql = """
    CREATE SCHEMA IF NOT EXISTS raw;
    CREATE SCHEMA IF NOT EXISTS staging;
    CREATE SCHEMA IF NOT EXISTS intermediate;
    CREATE SCHEMA IF NOT EXISTS analytics;

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
    """
    with engine.begin() as conn:
        conn.execute(text(init_sql))
    logging.info("Database schemas and raw tables initialized successfully.")


def bulk_upsert_raw_weather(records: List[Dict[str, Any]]) -> int:
    """Upsert raw weather observations into PostgreSQL raw schema."""
    if not records:
        logging.warning("No records provided for upsert.")
        return 0

    initialize_database()
    engine = get_db_engine()

    upsert_query = text("""
        INSERT INTO raw.weather_observations (
            city_name, country, latitude, longitude, timezone,
            observation_time, temperature_2m, relative_humidity_2m,
            apparent_temperature, precipitation, rain, weather_code,
            surface_pressure, wind_speed_10m, wind_direction_10m, raw_payload
        ) VALUES (
            :city_name, :country, :latitude, :longitude, :timezone,
            CAST(:observation_time AS TIMESTAMP), :temperature_2m, :relative_humidity_2m,
            :apparent_temperature, :precipitation, :rain, :weather_code,
            :surface_pressure, :wind_speed_10m, :wind_direction_10m, :raw_payload
        )
        ON CONFLICT (city_name, observation_time) DO UPDATE SET
            temperature_2m = EXCLUDED.temperature_2m,
            relative_humidity_2m = EXCLUDED.relative_humidity_2m,
            apparent_temperature = EXCLUDED.apparent_temperature,
            precipitation = EXCLUDED.precipitation,
            rain = EXCLUDED.rain,
            weather_code = EXCLUDED.weather_code,
            surface_pressure = EXCLUDED.surface_pressure,
            wind_speed_10m = EXCLUDED.wind_speed_10m,
            wind_direction_10m = EXCLUDED.wind_direction_10m,
            ingested_at = CURRENT_TIMESTAMP;
    """)

    formatted_records = []
    for r in records:
        row = dict(r)
        row["raw_payload"] = json.dumps(r)
        formatted_records.append(row)

    with engine.begin() as conn:
        conn.execute(upsert_query, formatted_records)

    logging.info(f"Successfully upserted {len(records)} weather observations into raw.weather_observations.")
    return len(records)
