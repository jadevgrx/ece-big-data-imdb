"""Measure null, missing and empty values in every raw IMDb table.

For each column of each table, three kinds of missing values are counted
separately, then combined into a single percentage:
    - real nulls (none expected, since the raw layer is all strings),
    - the IMDb null marker "\\N",
    - empty or whitespace-only strings.

Usage (inside the pyspark_notebook container):
    spark-submit --driver-memory 4g \\
        /home/jovyan/jobs/preparation/missing_values.py
    spark-submit /home/jovyan/jobs/preparation/missing_values.py --sample 0.01
"""

import argparse
import logging
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

IMDB_TABLES = [
    "name_basics",
    "title_akas",
    "title_basics",
    "title_crew",
    "title_episode",
    "title_principals",
    "title_ratings",
]
IMDB_NULL_MARKER = "\\N"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("missing_values")


def missing_report(spark, df, table):
    """Return one row per column with its missing value counts."""
    aggregations = [F.count(F.lit(1)).alias("__total")]
    for column in df.columns:
        col = F.col(f"`{column}`")
        aggregations += [
            F.sum(F.when(col.isNull(), 1).otherwise(0))
            .alias(f"{column}__null"),
            F.sum(F.when(col == IMDB_NULL_MARKER, 1).otherwise(0))
            .alias(f"{column}__marker"),
            F.sum(F.when(F.trim(col) == "", 1).otherwise(0))
            .alias(f"{column}__empty"),
        ]
    counts = df.agg(*aggregations).first().asDict()
    total = counts["__total"]

    rows = []
    for column in df.columns:
        nulls = counts[f"{column}__null"]
        markers = counts[f"{column}__marker"]
        empties = counts[f"{column}__empty"]
        missing = nulls + markers + empties
        pct = round(100.0 * missing / total, 4) if total else 0.0
        rows.append(
            (table, column, total, nulls, markers, empties, missing, pct)
        )
    return spark.createDataFrame(
        rows,
        "table string, column string, total_rows long, nulls long, "
        "null_markers long, empty_strings long, missing long, "
        "missing_pct double",
    )


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--data-dir",
        default="/home/jovyan/data",
        help="Root data directory (default: %(default)s)",
    )
    parser.add_argument(
        "--sample",
        type=float,
        default=None,
        help="Fraction of rows to use for a quick test, e.g. 0.01",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=5,
        help="Number of worst columns to display (default: %(default)s)",
    )
    return parser.parse_args()


def main():
    """Compute the report for every table and save it."""
    args = parse_args()
    data_dir = Path(args.data_dir)
    raw_dir = data_dir / "imdb" / "raw"
    output_dir = data_dir / "preparation" / "missing_values"

    spark = (
        SparkSession.builder
        .appName("missing_values")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    reports = []
    for table in IMDB_TABLES:
        df = spark.read.parquet(str(raw_dir / table))
        if args.sample:
            df = df.sample(fraction=args.sample, seed=42)
        log.info("Analysing %s", table)
        reports.append(missing_report(spark, df, table))

    report = reports[0]
    for other in reports[1:]:
        report = report.unionByName(other)
    report = report.orderBy(F.desc("missing_pct")).cache()

    print("\n=== Missing values per column (all tables) ===")
    report.show(100, truncate=False)

    print(f"\n=== Top {args.top} columns with the most missing values ===")
    report.limit(args.top).show(truncate=False)

    report.coalesce(1).write.mode("overwrite").option("header", True).csv(
        str(output_dir)
    )
    log.info("Report written to %s", output_dir)
    spark.stop()


if __name__ == "__main__":
    main()
