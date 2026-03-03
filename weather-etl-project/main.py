"""
Main orchestrator for the Weather ETL Pipeline.

Usage:
    python main.py              # Single run
    python main.py --schedule   # Run every 1 hour
"""

import argparse
import sys
import time
import logging
from datetime import datetime
from typing import Any

from config import CITIES, setup_logging
from etl.extract import extract_all_cities
from etl.transform import transform_weather_data
from etl.validate import run_all_validations, ValidationError
from etl.load import load_all

logger: logging.Logger = setup_logging()


def print_performance_summary(metrics: dict[str, Any]) -> None:
    border = "=" * 60
    summary_lines = [
        "",
        border,
        "  PIPELINE PERFORMANCE SUMMARY",
        border,
        f"  Status             : {metrics.get('status', 'UNKNOWN')}",
        f"  Execution Time     : {metrics.get('execution_time_sec', 0):.2f} seconds",
        f"  Cities Requested   : {metrics.get('cities_requested', 0)}",
        f"  Records Extracted  : {metrics.get('records_extracted', 0)}",
        f"  Records Rejected   : {metrics.get('records_rejected', 0)}",
        f"  dim_city Inserted  : {metrics.get('dim_city', 0)}",
        f"  dim_date Inserted  : {metrics.get('dim_date', 0)}",
        f"  fact_weather Ins.  : {metrics.get('fact_weather', 0)}",
        f"  Total Inserted     : {metrics.get('total_inserted', 0)}",
        border,
        "",
    ]
    summary = "\n".join(summary_lines)
    print(summary)
    logger.info("Performance Summary:\n%s", summary)


def run_etl_pipeline() -> None:
    pipeline_start: float = time.perf_counter()
    start_ts: str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    metrics: dict[str, Any] = {
        "status": "FAILED",
        "execution_time_sec": 0.0,
        "cities_requested": len(CITIES),
        "records_extracted": 0,
        "records_rejected": 0,
        "dim_city": 0,
        "dim_date": 0,
        "fact_weather": 0,
        "total_inserted": 0,
    }

    logger.info("=" * 60)
    logger.info("ETL PIPELINE STARTED at %s", start_ts)
    logger.info("=" * 60)

    try:
        raw_data = extract_all_cities(CITIES)
    except Exception as exc:
        logger.critical("Extraction phase failed: %s", exc)
        metrics["execution_time_sec"] = time.perf_counter() - pipeline_start
        print_performance_summary(metrics)
        return

    metrics["records_extracted"] = len(raw_data)
    metrics["records_rejected"] = len(CITIES) - len(raw_data)

    if not raw_data:
        logger.warning("No data extracted. Pipeline aborted.")
        metrics["execution_time_sec"] = time.perf_counter() - pipeline_start
        print_performance_summary(metrics)
        return

    try:
        city_df, date_df, fact_df = transform_weather_data(raw_data)
    except Exception as exc:
        logger.critical("Transformation phase failed: %s", exc)
        metrics["execution_time_sec"] = time.perf_counter() - pipeline_start
        print_performance_summary(metrics)
        return

    try:
        run_all_validations(city_df, fact_df)
    except ValidationError as exc:
        logger.critical("Validation phase failed: %s", exc)
        metrics["records_rejected"] = len(raw_data)
        metrics["execution_time_sec"] = time.perf_counter() - pipeline_start
        print_performance_summary(metrics)
        return

    try:
        row_counts = load_all(city_df, date_df, fact_df)
        metrics["dim_city"] = row_counts.get("dim_city", 0)
        metrics["dim_date"] = row_counts.get("dim_date", 0)
        metrics["fact_weather"] = row_counts.get("fact_weather", 0)
        metrics["total_inserted"] = row_counts.get("total_inserted", 0)
    except Exception as exc:
        logger.critical("Load phase failed: %s", exc)
        metrics["execution_time_sec"] = time.perf_counter() - pipeline_start
        print_performance_summary(metrics)
        return

    elapsed: float = time.perf_counter() - pipeline_start
    metrics["status"] = "SUCCESS"
    metrics["execution_time_sec"] = elapsed

    logger.info("=" * 60)
    logger.info("Pipeline completed successfully in %.2f seconds", elapsed)
    logger.info("=" * 60)

    print_performance_summary(metrics)


def run_scheduled() -> None:
    try:
        from apscheduler.schedulers.blocking import BlockingScheduler
    except ImportError:
        logger.error(
            "APScheduler is not installed. Install it with: pip install apscheduler"
        )
        sys.exit(1)

    scheduler = BlockingScheduler()
    scheduler.add_job(
        run_etl_pipeline, "interval", hours=1, next_run_time=datetime.now()
    )
    logger.info("Scheduler started — ETL will run every 1 hour.")

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped by user.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Weather ETL Pipeline")
    parser.add_argument(
        "--schedule",
        action="store_true",
        help="Run the pipeline on a recurring 1-hour schedule.",
    )
    args = parser.parse_args()

    if args.schedule:
        run_scheduled()
    else:
        run_etl_pipeline()
