"""
Extraction module for Weather ETL Pipeline.
"""

import logging
from typing import Any

import requests

from config import OPENWEATHER_API_KEY, OPENWEATHER_BASE_URL

logger = logging.getLogger("weather_etl")


def extract_weather_data(city: str) -> dict[str, Any]:
    if not OPENWEATHER_API_KEY:
        logger.error("OPENWEATHER_API_KEY environment variable is not set.")
        raise ValueError(
            "Missing API key. Set the OPENWEATHER_API_KEY environment variable."
        )

    params: dict[str, str] = {
        "q": city,
        "appid": OPENWEATHER_API_KEY,
    }

    logger.info("Extracting weather data for city: %s", city)

    try:
        response = requests.get(OPENWEATHER_BASE_URL, params=params, timeout=30)
        response.raise_for_status()
    except requests.exceptions.HTTPError as http_err:
        logger.error(
            "HTTP error for city '%s': %s (status %s)",
            city,
            http_err,
            response.status_code,
        )
        raise
    except requests.exceptions.RequestException as req_err:
        logger.error("Request failed for city '%s': %s", city, req_err)
        raise

    try:
        data: dict[str, Any] = response.json()
    except ValueError:
        logger.error("Invalid JSON response for city '%s'.", city)
        raise ValueError(f"Invalid JSON response received for city '{city}'.")

    logger.info("Successfully extracted data for city: %s", city)
    return data


def extract_all_cities(cities: list[str]) -> list[dict[str, Any]]:
    logger.info("Starting extraction for %d cities.", len(cities))
    results: list[dict[str, Any]] = []

    for city in cities:
        try:
            data = extract_weather_data(city)
            results.append(data)
        except Exception as exc:
            logger.warning("Skipping city '%s' due to error: %s", city, exc)

    logger.info(
        "Extraction complete. Successfully fetched data for %d / %d cities.",
        len(results),
        len(cities),
    )
    return results
