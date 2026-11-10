"""Q7: actors and actresses with a long career whose best movies came early.

Definitions (all thresholds are parameters):
- only titles of type movie with a rating and at least --min-votes votes
  are counted, so that the ratings compared are reliable;
- only the actor and actress categories of title.principals are used;
- an eligible career spans at least --min-span years (last counted movie
  year minus first) and has at least --min-movies counted movies;
- each movie gets a career position between 0 (first year) and 1 (last
  year). The early part of the career is the first --early-share of it
  (one third by default);
- a career peaked early when (1) the best-rated movie is in the early part
  (ties broken by votes, then by the earliest year) and (2) the average
  rating of the early movies is at least --min-drop points above the
  average of the later ones, with at least --min-early movies in the early
  part and --min-late movies after it, so that neither average rests on
  one or two films.

As a control, the symmetric "peaked late" count is also printed (best
movie in the last part of the career, later average above the early one
by --min-drop), to see whether early peaks are more common than late ones.

The slope of rating against year (regr_slope) is also given for every
eligible actor as a third, independent view of the trend.

Usage (inside the pyspark_notebook container, from /home/jovyan):
    spark-submit --driver-memory 4g jobs/exploration/q7_early_peak.py
"""

import argparse

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

IMDB_NULL = "\\N"


def read_raw(spark, raw_dir, table, columns):
    """Read some columns of a raw table, turning "\\N" into real nulls."""
    df = spark.read.parquet(f"{raw_dir}/{table}").select(*columns)
    return df.select([
        F.when((F.col(c) == IMDB_NULL) | (F.trim(F.col(c)) == ""), None)
        .otherwise(F.col(c))
        .alias(c)
        for c in columns
    ])


def safe_cast(column, data_type):
    """Cast returning null instead of failing (Spark 4 runs in ANSI mode)."""
    return F.expr(f"try_cast(`{column}` AS {data_type})")


def counted_movies(basics, ratings, min_votes):
    """Movies with a year and a reliable rating."""
    return (
        basics.filter(F.col("titleType") == "movie")
        .withColumn("startYear", safe_cast("startYear", "INT"))
        .filter(F.col("startYear").isNotNull())
        .join(ratings, on="tconst", how="inner")
        .withColumn("averageRating", safe_cast("averageRating", "DOUBLE"))
        .withColumn("numVotes", safe_cast("numVotes", "BIGINT"))
        .filter(F.col("averageRating").isNotNull()
                & (F.col("numVotes") >= min_votes))
        .select("tconst", "primaryTitle", "startYear", "averageRating",
                "numVotes")
    )


def actor_movies(principals, movies):
    """One row per (actor, movie) for the actor and actress categories."""
    return (
        principals.filter(F.col("category").isin("actor", "actress"))
        .select("tconst", "nconst").distinct()
        .join(movies, on="tconst", how="inner")
    )


def with_career_position(rows, min_span, min_movies):
    """Keep long careers and add each movie's position in the career."""
    by_actor = Window.partitionBy("nconst")
    return (
        rows
        .withColumn("first_year", F.min("startYear").over(by_actor))
        .withColumn("last_year", F.max("startYear").over(by_actor))
        .withColumn("n_movies", F.count("*").over(by_actor))
        .withColumn("span", F.col("last_year") - F.col("first_year"))
        .filter((F.col("span") >= min_span)
                & (F.col("n_movies") >= min_movies))
        .withColumn("position",
                    (F.col("startYear") - F.col("first_year"))
                    / F.col("span"))
    )


def summarize(rows, early_share):
    """Per actor: best movie, early and late averages, rating trend."""
    best_order = Window.partitionBy("nconst").orderBy(
        F.desc("averageRating"), F.desc("numVotes"), F.col("startYear"))
    best = (
        rows.withColumn("rn", F.row_number().over(best_order))
        .filter(F.col("rn") == 1)
        .select("nconst",
                F.col("primaryTitle").alias("best_movie"),
                F.col("startYear").alias("best_year"),
                F.col("averageRating").alias("best_rating"),
                F.col("position").alias("best_position"))
    )
    early = F.col("position") <= early_share
    stats = (
        rows.groupBy("nconst", "first_year", "last_year", "n_movies")
        .agg(
            F.sum(F.when(early, 1).otherwise(0)).alias("n_early"),
            F.sum(F.when(~early, 1).otherwise(0)).alias("n_late"),
            F.avg(F.when(early, F.col("averageRating"))).alias("early_avg"),
            F.avg(F.when(~early, F.col("averageRating"))).alias("late_avg"),
            F.regr_slope("averageRating", "startYear").alias("trend"),
        )
        .withColumn("drop", F.col("early_avg") - F.col("late_avg"))
    )
    return stats.join(best, on="nconst", how="inner")


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--raw-dir", default="data/imdb/raw")
    parser.add_argument("--out-dir", default="data/exploration/q7")
    parser.add_argument("--min-votes", type=int, default=1000)
    parser.add_argument("--min-span", type=int, default=20)
    parser.add_argument("--min-movies", type=int, default=15)
    parser.add_argument("--early-share", type=float, default=1 / 3)
    parser.add_argument("--min-early", type=int, default=3)
    parser.add_argument("--min-late", type=int, default=3)
    parser.add_argument("--min-drop", type=float, default=0.5)
    parser.add_argument("--show", type=int, default=30)
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("q7_early_peak")
        .config("spark.sql.shuffle.partitions", "64")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    basics = read_raw(spark, args.raw_dir, "title_basics",
                      ["tconst", "titleType", "primaryTitle", "startYear"])
    ratings = read_raw(spark, args.raw_dir, "title_ratings",
                       ["tconst", "averageRating", "numVotes"])
    principals = read_raw(spark, args.raw_dir, "title_principals",
                          ["tconst", "nconst", "category"])
    names = read_raw(spark, args.raw_dir, "name_basics",
                     ["nconst", "primaryName"])

    movies = counted_movies(basics, ratings, args.min_votes)
    rows = with_career_position(actor_movies(principals, movies),
                                args.min_span, args.min_movies)
    summary = summarize(rows, args.early_share).join(
        names, on="nconst", how="left")
    summary.write.mode("overwrite").parquet(f"{args.out_dir}/eligible")
    summary = spark.read.parquet(f"{args.out_dir}/eligible")

    enough_films = ((F.col("n_early") >= args.min_early)
                    & (F.col("n_late") >= args.min_late))
    result = summary.filter(
        enough_films
        & (F.col("best_position") <= args.early_share)
        & (F.col("drop") >= args.min_drop)
    )
    peaked_late = summary.filter(
        enough_films
        & (F.col("best_position") >= 1 - args.early_share)
        & (-F.col("drop") >= args.min_drop)
    )
    result.write.mode("overwrite").parquet(f"{args.out_dir}/result")
    result = spark.read.parquet(f"{args.out_dir}/result")

    n_eligible = summary.count()
    n_result = result.count()
    n_negative = summary.filter(F.col("trend") < 0).count()
    n_late = peaked_late.count()
    print(f"Eligible careers (>= {args.min_span} years, "
          f">= {args.min_movies} movies with >= {args.min_votes} votes): "
          f"{n_eligible}")
    print(f"Peaked early: {n_result} "
          f"({100 * n_result / max(n_eligible, 1):.1f} %)")
    print(f"Peaked late (control): {n_late} "
          f"({100 * n_late / max(n_eligible, 1):.1f} %)")
    print(f"Eligible careers with a negative rating trend: {n_negative} "
          f"({100 * n_negative / max(n_eligible, 1):.1f} %)")
    print(f"Top {args.show} by drop between early and later average:")
    (
        result.orderBy(F.desc("drop"))
        .select("primaryName", "first_year", "last_year", "n_movies",
                "n_early", "n_late",
                F.round("early_avg", 2).alias("early_avg"),
                F.round("late_avg", 2).alias("late_avg"),
                F.round("drop", 2).alias("drop"),
                "best_movie", "best_year", "best_rating",
                F.round("trend", 3).alias("trend"))
        .show(args.show, truncate=30)
    )
    spark.stop()


if __name__ == "__main__":
    main()