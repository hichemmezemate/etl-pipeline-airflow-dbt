"""
Airflow DAG: End-to-End Weather ETL & ELT Pipeline with dbt & PostgreSQL
"""

import sys
from datetime import datetime, timedelta
import logging
import os
import subprocess
from airflow import DAG
from airflow.operators.python import PythonOperator

# Import custom ETL utilities
from utils.api_client import WeatherAPIClient
from utils.db_helpers import bulk_upsert_raw_weather

default_args = {
    "owner": "data_engineering_team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=10),
}


def task_extract_and_load():
    """Extract weather data from Open-Meteo API and load into raw Postgres schema."""
    logging.info("Starting Open-Meteo API extraction...")
    client = WeatherAPIClient()
    records = client.extract_all_cities()
    logging.info(f"Extracted {len(records)} records. Upserting into PostgreSQL...")
    count = bulk_upsert_raw_weather(records)
    logging.info(f"Ingestion completed. Inserted/Updated {count} records in raw schema.")
    return count


def task_run_dbt_models():
    """Trigger dbt run transformations (Staging -> Intermediate -> Analytics Marts)."""
    dbt_project_dir = os.getenv("DBT_PROJECT_DIR", "/opt/airflow/dbt_project")
    profiles_dir = os.getenv("DBT_PROFILES_DIR", dbt_project_dir)

    cmd = [sys.executable, "-m", "dbt.cli.main", "run", "--project-dir", dbt_project_dir, "--profiles-dir", profiles_dir]
    logging.info(f"Executing dbt command: {' '.join(cmd)}")

    result = subprocess.run(cmd, capture_output=True, text=True)
    logging.info(f"dbt run stdout:\n{result.stdout}")

    if result.returncode != 0:
        logging.error(f"dbt run stderr:\n{result.stderr}")
        raise RuntimeError(f"dbt run failed: {result.stderr}")


def task_run_dbt_tests():
    """Trigger dbt test data quality assertions."""
    dbt_project_dir = os.getenv("DBT_PROJECT_DIR", "/opt/airflow/dbt_project")
    profiles_dir = os.getenv("DBT_PROFILES_DIR", dbt_project_dir)

    cmd = [sys.executable, "-m", "dbt.cli.main", "test", "--project-dir", dbt_project_dir, "--profiles-dir", profiles_dir]
    logging.info(f"Executing dbt test command: {' '.join(cmd)}")

    result = subprocess.run(cmd, capture_output=True, text=True)
    logging.info(f"dbt test stdout:\n{result.stdout}")

    if result.returncode != 0:
        logging.error(f"dbt test stderr:\n{result.stderr}")
        raise RuntimeError(f"dbt test failed: {result.stderr}")



def task_pipeline_summary():
    """Pipeline completion notification and health check summary."""
    logging.info("==================================================")
    logging.info("ETL & dbt Weather Pipeline Executed Successfully!")
    logging.info("Data flow: Open-Meteo API -> raw.weather_observations -> dbt Staging -> dbt Marts")
    logging.info("==================================================")



with DAG(
    dag_id="weather_etl_dbt_pipeline",
    default_args=default_args,
    description="End-to-end Weather Data Ingestion, PostgreSQL loading, and dbt Transformations",
    schedule_interval="0 */6 * * *",  # Run every 6 hours
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["etl", "weather", "dbt", "postgres"],
) as dag:

    t1_extract_load = PythonOperator(
        task_id="extract_and_load_raw_data",
        python_callable=task_extract_and_load,
    )

    t2_dbt_run = PythonOperator(
        task_id="run_dbt_transformations",
        python_callable=task_run_dbt_models,
    )

    t3_dbt_test = PythonOperator(
        task_id="run_dbt_data_quality_tests",
        python_callable=task_run_dbt_tests,
    )

    t4_summary = PythonOperator(
        task_id="pipeline_summary_and_metrics",
        python_callable=task_pipeline_summary,
    )

    t1_extract_load >> t2_dbt_run >> t3_dbt_test >> t4_summary
