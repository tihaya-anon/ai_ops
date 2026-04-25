import argparse
from datetime import datetime

from pyspark.sql import SparkSession


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate sample incidents dataset (practice).")
    parser.add_argument(
        "--dt",
        required=True,
        help="Partition date, e.g. 2026-04-25.",
    )
    parser.add_argument(
        "--output",
        default="/data/lake/processed/incidents",
        help="Output base path (Parquet).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    dt = args.dt

    datetime.strptime(dt, "%Y-%m-%d")

    spark = SparkSession.builder.appName(f"generate_sample_incidents_{dt}").getOrCreate()

    rows = [
        {"incident_id": "inc-001", "service": "checkout-service", "severity": "P0"},
        {"incident_id": "inc-002", "service": "checkout-service", "severity": "P1"},
        {"incident_id": "inc-003", "service": "mysql-order", "severity": "P0"},
        {"incident_id": "inc-004", "service": "search-service", "severity": "P2"},
        {"incident_id": "inc-005", "service": "checkout-service", "severity": "P0"},
    ]
    df = spark.createDataFrame(rows)

    out_path = f"{args.output}/dt={dt}"
    df.write.mode("overwrite").parquet(out_path)

    spark.stop()


if __name__ == "__main__":
    main()

