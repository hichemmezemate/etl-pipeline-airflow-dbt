"""
Open-Meteo API Client for Fetching Real-time and Forecast Weather Data.
"""

import logging
import time
from typing import Any, Dict, List
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Configured Target Cities with Lat/Long Coordinates
TARGET_CITIES = [
    {"name": "Paris", "country": "France", "latitude": 48.8566, "longitude": 2.3522},
    {"name": "Lyon", "country": "France", "latitude": 45.7640, "longitude": 4.8357},
    {"name": "Marseille", "country": "France", "latitude": 43.2965, "longitude": 5.3698},
    {"name": "Toulouse", "country": "France", "latitude": 43.6047, "longitude": 1.4442},
    {"name": "Nice", "country": "France", "latitude": 43.7102, "longitude": 7.2620},
    {"name": "Bruxelles", "country": "Belgique", "latitude": 50.8503, "longitude": 4.3517},
    {"name": "Geneve", "country": "Suisse", "latitude": 46.2044, "longitude": 6.1432},
]

OPEN_METEO_BASE_URL = "https://api.open-meteo.com/v1/forecast"


class WeatherAPIClient:
    """Client for extracting hourly weather metrics from Open-Meteo REST API."""

    def __init__(self, cities: List[Dict[str, Any]] = None, timeout: int = 15):
        self.cities = cities or TARGET_CITIES
        self.timeout = timeout

    def fetch_city_weather(self, city: Dict[str, Any], max_retries: int = 3) -> Dict[str, Any]:
        """Fetch hourly weather dataset for a single city with retry logic."""
        params = {
            "latitude": city["latitude"],
            "longitude": city["longitude"],
            "hourly": [
                "temperature_2m",
                "relative_humidity_2m",
                "apparent_temperature",
                "precipitation",
                "rain",
                "weather_code",
                "surface_pressure",
                "wind_speed_10m",
                "wind_direction_10m",
            ],
            "timezone": "auto",
            "forecast_days": 3,
        }

        for attempt in range(1, max_retries + 1):
            try:
                response = requests.get(OPEN_METEO_BASE_URL, params=params, timeout=self.timeout)
                response.raise_for_status()
                payload = response.json()
                logging.info(f"Successfully fetched weather data for {city['name']}")
                return payload
            except requests.RequestException as exc:
                logging.warning(f"Attempt {attempt}/{max_retries} failed for {city['name']}: {exc}")
                if attempt == max_retries:
                    raise RuntimeError(f"Failed to fetch data for {city['name']} after {max_retries} attempts.") from exc
                time.sleep(2 ** attempt)

    def extract_all_cities(self) -> List[Dict[str, Any]]:
        """Extract and parse weather records for all configured target cities."""
        all_records = []

        for city in self.cities:
            payload = self.fetch_city_weather(city)
            hourly = payload.get("hourly", {})
            times = hourly.get("time", [])

            for i, obs_time in enumerate(times):
                record = {
                    "city_name": city["name"],
                    "country": city["country"],
                    "latitude": city["latitude"],
                    "longitude": city["longitude"],
                    "timezone": payload.get("timezone", "UTC"),
                    "observation_time": obs_time.replace("T", " "),
                    "temperature_2m": hourly.get("temperature_2m", [])[i],
                    "relative_humidity_2m": hourly.get("relative_humidity_2m", [])[i],
                    "apparent_temperature": hourly.get("apparent_temperature", [])[i],
                    "precipitation": hourly.get("precipitation", [])[i],
                    "rain": hourly.get("rain", [])[i],
                    "weather_code": hourly.get("weather_code", [])[i],
                    "surface_pressure": hourly.get("surface_pressure", [])[i],
                    "wind_speed_10m": hourly.get("wind_speed_10m", [])[i],
                    "wind_direction_10m": hourly.get("wind_direction_10m", [])[i],
                }
                all_records.append(record)

        logging.info(f"Total parsed records extracted across {len(self.cities)} cities: {len(all_records)}")
        return all_records


if __name__ == "__main__":
    client = WeatherAPIClient()
    data = client.extract_all_cities()
    print(f"Extracted {len(data)} weather data rows successfully.")
