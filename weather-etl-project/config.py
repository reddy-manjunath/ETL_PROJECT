"""
Configuration module for Weather ETL Pipeline.
"""

import os
import logging

OPENWEATHER_API_KEY: str = os.environ.get("OPENWEATHER_API_KEY", "")
OPENWEATHER_BASE_URL: str = "https://api.openweathermap.org/data/2.5/weather"

CITIES: list[str] = ["Delhi", "Mumbai", "Bangalore", "Chennai", "Kolkata"]

DB_CONFIG: dict = {
    "host": os.environ.get("PG_HOST", "localhost"),
    "port": os.environ.get("PG_PORT", "5432"),
    "database": os.environ.get("PG_DATABASE", "weather_dw"),
    "user": os.environ.get("PG_USER", "postgres"),
    "password": os.environ.get("PG_PASSWORD", "postgres"),
}

LOG_DIR: str = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
LOG_FILE: str = os.path.join(LOG_DIR, "etl.log")
os.makedirs(LOG_DIR, exist_ok=True)


def setup_logging() -> logging.Logger:
    logger = logging.getLogger("weather_etl")
    logger.setLevel(logging.DEBUG)

    if logger.handlers:
        return logger

    file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(module)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger
