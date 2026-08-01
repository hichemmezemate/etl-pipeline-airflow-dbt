"""
Unit tests for Weather API Client.
"""

from unittest.mock import MagicMock, patch
import pytest
from airflow.dags.utils.api_client import WeatherAPIClient


def test_weather_api_client_init():
    client = WeatherAPIClient()
    assert len(client.cities) > 0
    assert client.cities[0]["name"] == "Paris"


@patch("airflow.dags.utils.api_client.requests.get")
def test_fetch_city_weather_success(mock_get):
    mock_response = MagicMock()
    mock_response.json.return_value = {
        "timezone": "Europe/Paris",
        "hourly": {
            "time": ["2025-01-01T00:00"],
            "temperature_2m": [12.5],
            "relative_humidity_2m": [80],
            "apparent_temperature": [11.0],
            "precipitation": [0.0],
            "rain": [0.0],
            "weather_code": [0],
            "surface_pressure": [1013.25],
            "wind_speed_10m": [15.0],
            "wind_direction_10m": [180],
        },
    }
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response

    client = WeatherAPIClient(cities=[{"name": "Paris", "country": "France", "latitude": 48.85, "longitude": 2.35}])
    data = client.extract_all_cities()

    assert len(data) == 1
    assert data[0]["city_name"] == "Paris"
    assert data[0]["temperature_2m"] == 12.5
