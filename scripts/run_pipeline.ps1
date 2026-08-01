# Powershell Runner script for testing pipeline execution locally

$ErrorActionPreference = "Stop"

Write-Host "==================================================" -ForegroundColor Cyan
Write-Host "Lancement du Pipeline ETL Weather (Airflow/dbt/Postgres)" -ForegroundColor Cyan
Write-Host "==================================================" -ForegroundColor Cyan

# 1. Verification / Ingestion Python
Write-Host "`n1. Ingestion de l'API Open-Meteo vers PostgreSQL..." -ForegroundColor Yellow
$env:PYTHONPATH = "airflow/dags"
python -c "from utils.api_client import WeatherAPIClient; from utils.db_helpers import bulk_upsert_raw_weather; client = WeatherAPIClient(); records = client.extract_all_cities(); bulk_upsert_raw_weather(records)"

# 2. Execution des transformations dbt
Write-Host "`n2. Exécution des transformations dbt SQL (Staging -> Marts)..." -ForegroundColor Yellow
$env:DBT_PROFILES_DIR = "dbt_project"
dbt run --project-dir dbt_project --profiles-dir dbt_project

# 3. Execution des tests de qualite dbt
Write-Host "`n3. Exécution des tests de qualité dbt (Assertions & Data Integrity)..." -ForegroundColor Yellow
dbt test --project-dir dbt_project --profiles-dir dbt_project

Write-Host "`nPipeline execute avec succes!" -ForegroundColor Green
