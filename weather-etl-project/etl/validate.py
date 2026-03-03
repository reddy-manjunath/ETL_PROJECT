"""
Validation module for Weather ETL Pipeline.

Performs data-quality checks on the transformed DataFrames
before they are loaded into the data warehouse.
"""

import logging

import pandas as pd

logger = logging.getLogger("weather_etl")


class ValidationError(Exception):
    """Raised when one or more data-quality checks fail."""


def validate_temperature(fact_df: pd.DataFrame) -> bool:

    out_of_range = fact_df[
        (fact_df["temperature_celsius"] < -50)
        | (fact_df["temperature_celsius"] > 60)
    ]
    if not out_of_range.empty:
        msg = f"Temperature out of range for {len(out_of_range)} record(s)."
        logger.error(msg)
        raise ValidationError(msg)

    logger.info("✓ Temperature validation passed.")
    return True


def validate_humidity(fact_df: pd.DataFrame) -> bool:

    out_of_range = fact_df[
        (fact_df["humidity"] < 0) | (fact_df["humidity"] > 100)
    ]
    if not out_of_range.empty:
        msg = f"Humidity out of range for {len(out_of_range)} record(s)."
        logger.error(msg)
        raise ValidationError(msg)

    logger.info("✓ Humidity validation passed.")
    return True


def validate_city_not_null(city_df: pd.DataFrame) -> bool:

    null_cities = city_df[
        city_df["city_name"].isnull() | (city_df["city_name"].str.strip() == "")
    ]
    if not null_cities.empty:
        msg = f"Found {len(null_cities)} record(s) with null/empty city name."
        logger.error(msg)
        raise ValidationError(msg)

    logger.info("✓ City name null-check passed.")
    return True


def validate_no_duplicate_cities(city_df: pd.DataFrame) -> bool:

    duplicates = city_df[city_df.duplicated(subset=["city_name"], keep=False)]
    if not duplicates.empty:
        dup_names = duplicates["city_name"].unique().tolist()
        msg = f"Duplicate city entries found: {dup_names}"
        logger.error(msg)
        raise ValidationError(msg)

    logger.info("✓ No duplicate cities found.")
    return True


def run_all_validations(city_df: pd.DataFrame,fact_df: pd.DataFrame) -> bool:

    logger.info("Running data validations …")

    validate_temperature(fact_df)
    validate_humidity(fact_df)
    validate_city_not_null(city_df)
    validate_no_duplicate_cities(city_df)

    logger.info("All validations passed successfully.")
    return True
