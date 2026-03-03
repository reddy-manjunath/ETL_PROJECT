"""
Transformation module for Weather ETL Pipeline.

Converts raw JSON weather data into structured pandas DataFrames
aligned with the star schema (dim_city, dim_date, fact_weather).
"""

import logging
from datetime import datetime, timezone
from typing import Any

import pandas as pd

logger = logging.getLogger("weather_etl")


def _kelvin_to_celsius(kelvin: float) -> float:
    return round(kelvin - 273.15, 2)


def _unix_to_datetime(unix_ts: int) -> datetime:
    return datetime.fromtimestamp(unix_ts, tz=timezone.utc)


def transform_weather_data(
    raw_data_list: list[dict[str, Any]],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    logger.info("Starting transformation of %d records.", len(raw_data_list))

    city_records: list[dict[str, Any]] = []
    date_records: list[dict[str, Any]] = []
    fact_records: list[dict[str, Any]] = []

    for raw in raw_data_list:
        try:
            # --- City dimension ---
            city_name: str = raw["name"]
            country: str = raw["sys"]["country"]
            latitude: float = raw["coord"]["lat"]
            longitude: float = raw["coord"]["lon"]

            city_records.append(
                {
                    "city_name": city_name,
                    "country": country,
                    "latitude": latitude,
                    "longitude": longitude,
                }
            )

            # --- Date dimension ---
            dt: datetime = _unix_to_datetime(raw["dt"])

            date_records.append(
                {
                    "full_timestamp": dt,
                    "year": dt.year,
                    "month": dt.month,
                    "day": dt.day,
                    "hour": dt.hour,
                }
            )

            # --- Fact record ---
            temperature_celsius: float = _kelvin_to_celsius(raw["main"]["temp"])
            humidity: int = raw["main"]["humidity"]
            pressure: int = raw["main"]["pressure"]
            wind_speed: float = raw["wind"]["speed"]

            fact_records.append(
                {
                    "city_name": city_name,
                    "full_timestamp": dt,
                    "temperature_celsius": temperature_celsius,
                    "humidity": humidity,
                    "pressure": pressure,
                    "wind_speed": wind_speed,
                }
            )

        except KeyError as key_err:
            logger.warning(
                "Skipping record due to missing key: %s | raw=%s", key_err, raw
            )

    city_df = pd.DataFrame(city_records).drop_duplicates(subset=["city_name"])
    date_df = pd.DataFrame(date_records).drop_duplicates(subset=["full_timestamp"])
    fact_df = pd.DataFrame(fact_records)

    logger.info(
        "Transformation complete — cities: %d, dates: %d, facts: %d",
        len(city_df),
        len(date_df),
        len(fact_df),
    )

    return city_df, date_df, fact_df
