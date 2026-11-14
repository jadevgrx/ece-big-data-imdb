"""Q4: directors with exactly one great movie and a weak rest of career.

A director qualifies when, among his feature-length movies (titleType =
"movie", runtime >= 40 minutes) with at least --min-votes votes:
    - exactly one movie has an average rating >= 8.0,
    - at least two other movies exist,
    - the simple mean rating of those other movies is <= 6.0.

The vote threshold avoids ratings given by a handful of users, which are
often extreme. Directors come from title_crew (all credited directors,
co-directors included).

Usage (inside the pyspark_notebook container):
    spark-submit --driver-memory 4g \\
        /home/jovyan/jobs/exploration/q4_one_hit_directors.py
    spark-submit /home/jovyan/jobs/exploration/q4_one_hit_directors.py \\
        --min-votes 100
"""

import argparse
import logging
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

MIN_RUNTIME = 40
HIT_RATING = 8.0
MAX_OTHERS_AVG = 6.0
MIN_OTHER_MOVIES = 2

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("q4_one_hit_directors")


def directed_movies(titles, ratings, crew, min_votes):
    """Return one row per (director, rated feature-length movie)."""
    movies = (
        titles
        .filter(
            (F.col("titleType") == "movie")
            & (F.col("runtimeMinutes") >= MIN_RUNTIME)
        )
        .join(ratings.filter(F.col("numVotes") >= min_votes), "tconst")
        .select("tconst", "primaryTitle", "startYear", "averageRating",
                "numVotes")
    )
    directors = crew.select(
        "tconst", F.explode("directors").alias("nconst"))
    return movies.join(directors, "tconst")


def one_hit_directors(movies):
    """Keep the directors matching the Q4 rule, with their career stats."""
    is_hit = F.col("averageRating") >= HIT_RATING
    careers = movies.groupBy("nconst").agg(
        F.count("*").alias("n_movies"),
        F.sum(F.when(is_hit, 1).otherwise(0)).alias("n_hits"),
        F.avg(F.when(~is_hit, F.col("averageRating"))).alias("others_avg"),
        F.min(F.when(~is_hit, F.col("averageRating"))).alias("others_min"),
        F.max(F.when(~is_hit, F.col("averageRating"))).alias("others_max"),
    )
    selected = careers.filter(
        (F.col("n_hits") == 1)
        & (F.col("n_movies") - 1 >= MIN_OTHER_MOVIES)
        & (F.col("others_avg") <= MAX_OTHERS_AVG)
    )
    hits = movies.filter(is_hit).select(
        "nconst",
        F.col("primaryTitle").alias("hit_title"),
        F.col("startYear").alias("hit_year"),
        F.col("averageRating").alias("hit_rating"),
        F.col("numVotes").alias("hit_votes"),
    )
    return (
        selected.join(hits, "nconst")
        .withColumn("n_others", F.col("n_movies") - 1)
        .withColumn("others_avg", F.round("others_avg", 2))
        .withColumn("gap", F.round(F.col("hit_rating")
                                   - F.col("others_avg"), 2))
        .drop("n_hits", "n_movies")
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
        "--min-votes",
        type=int,
        default=1000,
        help="Minimum votes for a movie to be counted (default: %(default)s)",
    )
    return parser.parse_args()


def main():
    """Run Q4 and save the list of directors."""
    args = parse_args()
    data_dir = Path(args.data_dir)
    clean_dir = data_dir / "imdb" / "clean"
    output_dir = (data_dir / "exploration" / "q4"
                  / f"min_votes_{args.min_votes}")

    spark = (
        SparkSession.builder
        .appName("q4_one_hit_directors")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    def read(name):
        return spark.read.parquet(str(clean_dir / name))

    movies = directed_movies(read("title_basics"), read("title_ratings"),
                             read("title_crew"), args.min_votes).cache()
    names = read("name_basics").select("nconst", "primaryName")
    result = (
        one_hit_directors(movies)
        .join(names, "nconst")
        .select(
            "nconst", "primaryName", "hit_title", "hit_year", "hit_rating",
            "hit_votes", "n_others", "others_avg", "others_min",
            "others_max", "gap",
        )
        .cache()
    )

    n_directors = movies.select("nconst").distinct().count()
    n_result = result.count()
    log.info("min_votes = %d: %d directors with at least one movie, "
             "%d match Q4", args.min_votes, n_directors, n_result)

    print(f"\n=== Q4 one-hit directors (min votes {args.min_votes}), "
          "most voted hits first ===")
    result.orderBy(F.desc("hit_votes")).show(30, truncate=False)
    print("\n=== Q4 largest gaps between the hit and the rest ===")
    result.orderBy(F.desc("gap")).show(10, truncate=False)

    result.coalesce(1).write.mode("overwrite").option("header", True).csv(
        str(output_dir))
    log.info("Results written to %s", output_dir)
    spark.stop()


if __name__ == "__main__":
    main()
