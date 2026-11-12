"""Q1: feature-length movies whose runtime is far from their genre average.

A feature-length movie is a title with titleType = "movie" and a runtime
of at least 40 minutes (definition shared by the whole report). A movie
with several genres is compared to each of its genres.

Two outlier rules are available:
    - zscore (default): runtime outside mean +/- k * standard deviation,
      with k = 3,
    - iqr: runtime outside [Q1 - k * IQR, Q3 + k * IQR], with k = 1.5,
      more robust because quartiles are not pulled by extreme values.

Genre statistics are computed without the movies flagged
is_extreme_runtime (over 1000 minutes) so that a handful of experimental
films do not inflate the standard deviation; those movies are still
tested and come out as outliers. Announced movies (is_future_release)
are excluded since their runtime is not final.

Usage (inside the pyspark_notebook container):
    spark-submit --driver-memory 4g /home/jovyan/jobs/exploration/q1_runtime_outliers.py
    spark-submit /home/jovyan/jobs/exploration/q1_runtime_outliers.py --method iqr
"""

import argparse
import logging
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

MIN_RUNTIME = 40
DEFAULT_K = {"zscore": 3.0, "iqr": 1.5}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("q1_runtime_outliers")


def feature_movies(titles):
    """Return one row per (feature-length movie, genre)."""
    return (
        titles
        .filter(
            (F.col("titleType") == "movie")
            & (F.col("runtimeMinutes") >= MIN_RUNTIME)
            & ~F.col("is_future_release")
            & F.col("genres").isNotNull()
        )
        .select(
            "tconst",
            "primaryTitle",
            "startYear",
            "runtimeMinutes",
            "is_extreme_runtime",
            F.explode("genres").alias("genre"),
        )
    )


def genre_bounds(movies, method, k):
    """Compute per genre statistics and the accepted runtime range."""
    reference = movies.filter(~F.col("is_extreme_runtime"))
    stats = reference.groupBy("genre").agg(
        F.count("*").alias("n_movies"),
        F.round(F.avg("runtimeMinutes"), 1).alias("mean_runtime"),
        F.round(F.stddev_samp("runtimeMinutes"), 1).alias("std_runtime"),
        F.percentile_approx("runtimeMinutes", [0.25, 0.5, 0.75])
        .alias("quartiles"),
    )
    stats = stats.select(
        "genre",
        "n_movies",
        "mean_runtime",
        "std_runtime",
        F.col("quartiles")[0].alias("q1"),
        F.col("quartiles")[1].alias("median_runtime"),
        F.col("quartiles")[2].alias("q3"),
    )
    if method == "zscore":
        lower = F.col("mean_runtime") - k * F.col("std_runtime")
        upper = F.col("mean_runtime") + k * F.col("std_runtime")
    else:
        iqr = F.col("q3") - F.col("q1")
        lower = F.col("q1") - k * iqr
        upper = F.col("q3") + k * iqr
    return (
        stats
        .withColumn("lower_bound", F.round(lower, 1))
        .withColumn("upper_bound", F.round(upper, 1))
    )


def find_outliers(movies, bounds):
    """Keep the movies outside the accepted range of their genre."""
    return (
        movies.join(bounds, "genre")
        .filter(
            (F.col("runtimeMinutes") < F.col("lower_bound"))
            | (F.col("runtimeMinutes") > F.col("upper_bound"))
        )
        .withColumn(
            "z_score",
            F.round(
                (F.col("runtimeMinutes") - F.col("mean_runtime"))
                / F.col("std_runtime"),
                2,
            ),
        )
        .withColumn(
            "direction",
            F.when(F.col("runtimeMinutes") > F.col("upper_bound"), "longer")
            .otherwise("shorter"),
        )
        .select(
            "genre",
            "tconst",
            "primaryTitle",
            "startYear",
            "runtimeMinutes",
            "mean_runtime",
            "z_score",
            "direction",
            "is_extreme_runtime",
        )
    )


def genre_summary(bounds, outliers):
    """Count the outliers of each genre, split by direction."""
    counts = outliers.groupBy("genre").agg(
        F.count("*").alias("n_outliers"),
        F.sum(F.when(F.col("direction") == "longer", 1).otherwise(0))
        .alias("n_longer"),
        F.sum(F.when(F.col("direction") == "shorter", 1).otherwise(0))
        .alias("n_shorter"),
    )
    return (
        bounds.join(counts, "genre", "left")
        .fillna(0, ["n_outliers", "n_longer", "n_shorter"])
        .withColumn(
            "pct_outliers",
            F.round(100.0 * F.col("n_outliers") / F.col("n_movies"), 2),
        )
        .orderBy("genre")
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
        "--method",
        choices=["zscore", "iqr"],
        default="zscore",
        help="Outlier rule (default: %(default)s)",
    )
    parser.add_argument(
        "--k",
        type=float,
        default=None,
        help="Multiplier of the rule (default: 3 for zscore, 1.5 for iqr)",
    )
    return parser.parse_args()


def main():
    """Run Q1 and save the genre summary and the outlier list."""
    args = parse_args()
    k = args.k if args.k is not None else DEFAULT_K[args.method]
    data_dir = Path(args.data_dir)
    clean_dir = data_dir / "imdb" / "clean"
    output_dir = data_dir / "exploration" / "q1" / args.method

    spark = (
        SparkSession.builder
        .appName("q1_runtime_outliers")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    titles = spark.read.parquet(str(clean_dir / "title_basics"))
    movies = feature_movies(titles).cache()
    bounds = genre_bounds(movies, args.method, k)
    outliers = find_outliers(movies, bounds).cache()
    summary = genre_summary(bounds, outliers)

    n_movies = movies.select("tconst").distinct().count()
    n_outlier_movies = outliers.select("tconst").distinct().count()
    log.info("Method %s, k = %s", args.method, k)
    log.info("%d feature-length movies, %d are outliers in at least one "
             "genre", n_movies, n_outlier_movies)

    print(f"\n=== Q1 per genre ({args.method}, k = {k}) ===")
    summary.show(50, truncate=False)

    print("\n=== Q1 most extreme outliers (excluding > 1000 min) ===")
    (outliers.filter(~F.col("is_extreme_runtime"))
     .orderBy(F.desc(F.abs(F.col("z_score"))))
     .show(20, truncate=False))

    summary.coalesce(1).write.mode("overwrite").option("header", True).csv(
        str(output_dir / "genre_summary"))
    outliers.write.mode("overwrite").parquet(str(output_dir / "outliers"))
    log.info("Results written to %s", output_dir)
    spark.stop()


if __name__ == "__main__":
    main()