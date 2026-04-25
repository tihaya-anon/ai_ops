import argparse

from pyspark.sql import SparkSession, functions as F


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Daily incident aggregates (batch layer).")
    parser.add_argument(
        "--dt",
        required=True,
        help="Partition date, e.g. 2026-04-25. Used for input/output partitioning.",
    )
    parser.add_argument(
        "--input",
        default="/data/lake/processed/incidents",
        help="Input path (Parquet), partitioned by dt=YYYY-MM-DD.",
    )
    parser.add_argument(
        "--output",
        default="/data/lake/analytics/incident_daily",
        help="Output path (Parquet), partitioned by dt=YYYY-MM-DD.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    spark = (
        SparkSession.builder.appName(f"incident_daily_aggregates_{args.dt}")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )

    # Expected minimal schema (flexible for practice):
    # - incident_id: string
    # - service: string
    # - severity: string (P0/P1/P2...)
    # - created_at: timestamp (optional)
    input_path = f"{args.input}/dt={args.dt}"

    incidents = spark.read.parquet(input_path)

    base = incidents
    if "service" not in base.columns:
        base = base.withColumn("service", F.lit("unknown"))
    if "severity" not in base.columns:
        base = base.withColumn("severity", F.lit("unknown"))
    if "incident_id" not in base.columns:
        base = base.withColumn("incident_id", F.sha2(F.to_json(F.struct(*base.columns)), 256))

    daily = (
        base.groupBy("service", "severity")
        .agg(
            F.countDistinct("incident_id").alias("incident_cnt"),
            F.count(F.lit(1)).alias("row_cnt"),
        )
        .withColumn("dt", F.lit(args.dt))
    )

    (
        daily.repartition(1)
        .write.mode("overwrite")
        .partitionBy("dt")
        .parquet(args.output)
    )

    spark.stop()


if __name__ == "__main__":
    main()

