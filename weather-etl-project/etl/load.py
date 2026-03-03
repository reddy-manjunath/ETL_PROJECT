"""
Load module for Weather ETL Pipeline.
"""

import logging
from typing import Optional

import pandas as pd
import psycopg2
from psycopg2.extensions import connection as PgConnection

from config import DB_CONFIG

logger = logging.getLogger("weather_etl")


def get_db_connection() -> PgConnection:
    try:
        conn: PgConnection = psycopg2.connect(**DB_CONFIG)
        logger.info("Database connection established.")
        return conn
    except psycopg2.OperationalError as err:
        logger.error("Database connection failed: %s", err)
        raise


def load_dim_city(conn: PgConnection, city_df: pd.DataFrame) -> int:
    insert_query = """
        INSERT INTO dim_city (city_name, country, latitude, longitude)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (city_name) DO NOTHING;
    """
    rows_inserted = 0
    rows_skipped = 0

    try:
        with conn.cursor() as cur:
            for _, row in city_df.iterrows():
                cur.execute(
                    insert_query,
                    (row["city_name"], row["country"], row["latitude"], row["longitude"]),
                )
                if cur.rowcount > 0:
                    rows_inserted += cur.rowcount
                else:
                    rows_skipped += 1
        conn.commit()
        logger.info("Inserted %d rows into dim_city.", rows_inserted)
        if rows_skipped:
            logger.info("Skipped %d duplicate rows in dim_city.", rows_skipped)
    except Exception as exc:
        conn.rollback()
        logger.error("Failed to load dim_city: %s", exc)
        raise

    return rows_inserted


def load_dim_date(conn: PgConnection, date_df: pd.DataFrame) -> int:
    insert_query = """
        INSERT INTO dim_date (full_timestamp, year, month, day, hour)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (full_timestamp) DO NOTHING;
    """
    rows_inserted = 0
    rows_skipped = 0

    try:
        with conn.cursor() as cur:
            for _, row in date_df.iterrows():
                cur.execute(
                    insert_query,
                    (
                        row["full_timestamp"],
                        int(row["year"]),
                        int(row["month"]),
                        int(row["day"]),
                        int(row["hour"]),
                    ),
                )
                if cur.rowcount > 0:
                    rows_inserted += cur.rowcount
                else:
                    rows_skipped += 1
        conn.commit()
        logger.info("Inserted %d rows into dim_date.", rows_inserted)
        if rows_skipped:
            logger.info("Skipped %d duplicate rows in dim_date.", rows_skipped)
    except Exception as exc:
        conn.rollback()
        logger.error("Failed to load dim_date: %s", exc)
        raise

    return rows_inserted


def _get_city_id(conn: PgConnection, city_name: str) -> Optional[int]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT city_id FROM dim_city WHERE city_name = %s;", (city_name,)
        )
        result = cur.fetchone()
    return result[0] if result else None


def _get_date_id(conn: PgConnection, full_timestamp) -> Optional[int]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT date_id FROM dim_date WHERE full_timestamp = %s;",
            (full_timestamp,),
        )
        result = cur.fetchone()
    return result[0] if result else None


def load_fact_weather(conn: PgConnection, fact_df: pd.DataFrame) -> int:
    insert_query = """
        INSERT INTO fact_weather
            (city_id, date_id, temperature_celsius, humidity, pressure, wind_speed)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (city_id, date_id) DO NOTHING;
    """
    rows_inserted = 0
    rows_skipped = 0

    try:
        with conn.cursor() as cur:
            for _, row in fact_df.iterrows():
                city_id = _get_city_id(conn, row["city_name"])
                date_id = _get_date_id(conn, row["full_timestamp"])

                if city_id is None:
                    logger.warning("city_id not found for '%s'. Skipping.", row["city_name"])
                    rows_skipped += 1
                    continue
                if date_id is None:
                    logger.warning("date_id not found for timestamp '%s'. Skipping.", row["full_timestamp"])
                    rows_skipped += 1
                    continue

                cur.execute(
                    insert_query,
                    (
                        city_id,
                        date_id,
                        float(row["temperature_celsius"]),
                        int(row["humidity"]),
                        int(row["pressure"]),
                        float(row["wind_speed"]),
                    ),
                )
                if cur.rowcount > 0:
                    rows_inserted += cur.rowcount
                else:
                    rows_skipped += 1

        conn.commit()
        logger.info("Inserted %d rows into fact_weather.", rows_inserted)
        if rows_skipped:
            logger.info("Skipped %d rows in fact_weather (duplicates or missing keys).", rows_skipped)
    except Exception as exc:
        conn.rollback()
        logger.error("Failed to load fact_weather: %s", exc)
        raise

    return rows_inserted


def load_all(
    city_df: pd.DataFrame,
    date_df: pd.DataFrame,
    fact_df: pd.DataFrame,
) -> dict[str, int]:
    total_input = len(city_df) + len(date_df) + len(fact_df)
    logger.info("Total records to process: %d", total_input)

    conn = get_db_connection()
    try:
        city_count = load_dim_city(conn, city_df)
        date_count = load_dim_date(conn, date_df)
        fact_count = load_fact_weather(conn, fact_df)
    finally:
        conn.close()
        logger.info("Database connection closed.")

    total_inserted = city_count + date_count + fact_count
    logger.info(
        "Load complete — Total inserted: %d / %d records.",
        total_inserted,
        total_input,
    )

    return {
        "dim_city": city_count,
        "dim_date": date_count,
        "fact_weather": fact_count,
        "total_processed": total_input,
        "total_inserted": total_inserted,
    }
