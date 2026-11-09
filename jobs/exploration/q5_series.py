"""Q5: series whose worst episode is in the last season and best episode
in the first two seasons.

Definitions (all thresholds are parameters):
- a series is a title of type tvSeries or tvMiniSeries;
- an episode counts only if it has a rating with at least --min-votes votes;
- a qualifying season has at least --min-episodes counted episodes, and a
  series must have at least --min-seasons qualifying seasons. Only these
  seasons are compared, so specials (season 0) and unfinished seasons do
  not decide the result;
- with --ended-only, series still running (no end year) are left out,
  because their latest season is not necessarily their last one;
- the worst (best) episode is the one with the lowest (highest) rating;
  ties are broken by the number of votes, then by ID, so the result is
  deterministic.

Usage (inside the pyspark_notebook container, from /home/jovyan):
    spark-submit --driver-memory 4g jobs/exploration/q5_series.py
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


def rated_episodes(episodes, ratings, series, min_votes):
    """Episodes of real series with a reliable rating."""
    return (
        episodes
        .withColumn("seasonNumber", safe_cast("seasonNumber", "INT"))
        .withColumn("episodeNumber", safe_cast("episodeNumber", "INT"))
        .filter(F.col("seasonNumber").isNotNull()
                & (F.col("seasonNumber") > 0))
        .join(series.select(F.col("tconst").alias("parentTconst")),
              on="parentTconst", how="inner")
        .join(ratings, on="tconst", how="inner")
        .withColumn("averageRating", safe_cast("averageRating", "DOUBLE"))
        .withColumn("numVotes", safe_cast("numVotes", "BIGINT"))
        .filter(F.col("numVotes") >= min_votes)
    )


def qualifying_seasons(episodes, min_episodes, min_seasons):
    """Keep seasons with enough episodes, in series with enough seasons.

    Adds the rank of each season among the qualifying seasons of its
    series, from the first (1) and from the last (1).
    """
    seasons = (
        episodes.groupBy("parentTconst", "seasonNumber")
        .agg(F.count("*").alias("episodes_in_season"))
        .filter(F.col("episodes_in_season") >= min_episodes)
    )
    by_series = Window.partitionBy("parentTconst")
    first = by_series.orderBy("seasonNumber")
    last = by_series.orderBy(F.desc("seasonNumber"))
    return (
        seasons
        .withColumn("n_seasons", F.count("*").over(by_series))
        .filter(F.col("n_seasons") >= min_seasons)
        .withColumn("rank_from_first", F.row_number().over(first))
        .withColumn("rank_from_last", F.row_number().over(last))
    )


def extreme_episode(episodes, ascending, prefix):
    """Worst (ascending) or best episode of each series, with its season."""
    rating = F.col("averageRating")
    order = [rating.asc() if ascending else rating.desc(),
             F.desc("numVotes"), F.col("tconst")]
    window = Window.partitionBy("parentTconst").orderBy(*order)
    return (
        episodes
        .withColumn("rn", F.row_number().over(window))
        .filter(F.col("rn") == 1)
        .select(
            "parentTconst",
            F.col("tconst").alias(f"{prefix}_tconst"),
            F.col("seasonNumber").alias(f"{prefix}_season"),
            F.col("episodeNumber").alias(f"{prefix}_episode"),
            F.col("averageRating").alias(f"{prefix}_rating"),
            F.col("rank_from_first").alias(f"{prefix}_rank_from_first"),
            F.col("rank_from_last").alias(f"{prefix}_rank_from_last"),
        )
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--raw-dir", default="data/imdb/raw")
    parser.add_argument("--out-dir", default="data/exploration/q5")
    parser.add_argument("--min-episodes", type=int, default=10)
    parser.add_argument("--min-seasons", type=int, default=3)
    parser.add_argument("--min-votes", type=int, default=10)
    parser.add_argument("--show", type=int, default=25)
    parser.add_argument("--ended-only", action="store_true",
                        help="keep only series with an end year")
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("q5_series")
        .config("spark.sql.shuffle.partitions", "64")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    basics = read_raw(spark, args.raw_dir, "title_basics",
                      ["tconst", "titleType", "primaryTitle", "startYear",
                       "endYear"])
    ratings = read_raw(spark, args.raw_dir, "title_ratings",
                       ["tconst", "averageRating", "numVotes"])
    episodes = read_raw(spark, args.raw_dir, "title_episode",
                        ["tconst", "parentTconst", "seasonNumber",
                         "episodeNumber"])

    series = basics.filter(
        F.col("titleType").isin("tvSeries", "tvMiniSeries"))
    if args.ended_only:
        # A running series has no final season yet.
        series = series.filter(F.col("endYear").isNotNull())
    rated = rated_episodes(episodes, ratings, series, args.min_votes)
    seasons = qualifying_seasons(rated, args.min_episodes, args.min_seasons)

    # Only episodes from qualifying seasons are compared.
    kept = rated.join(seasons, on=["parentTconst", "seasonNumber"],
                      how="inner")
    worst = extreme_episode(kept, ascending=True, prefix="worst")
    best = extreme_episode(kept, ascending=False, prefix="best")

    series_info = (
        series.select(
            F.col("tconst").alias("parentTconst"),
            F.col("primaryTitle").alias("series_title"),
            "startYear", "endYear")
        .join(ratings.select(
            F.col("tconst").alias("parentTconst"),
            safe_cast("numVotes", "BIGINT").alias("series_votes")),
            on="parentTconst", how="left")
    )
    n_seasons = seasons.groupBy("parentTconst").agg(
        F.max("n_seasons").alias("qualifying_seasons"),
        F.max("seasonNumber").alias("last_season"))

    candidates = (
        n_seasons.join(worst, on="parentTconst")
        .join(best, on="parentTconst")
        .join(series_info, on="parentTconst", how="left")
    )
    result = candidates.filter(
        (F.col("worst_rank_from_last") == 1)
        & (F.col("best_rank_from_first") <= 2)
    )

    result.write.mode("overwrite").parquet(f"{args.out_dir}/result")
    result = spark.read.parquet(f"{args.out_dir}/result")
    n_candidates = candidates.count()
    n_result = result.count()

    print(f"Series with >= {args.min_seasons} seasons of "
          f">= {args.min_episodes} rated episodes: {n_candidates}")
    print(f"Matching series: {n_result} "
          f"({100 * n_result / max(n_candidates, 1):.1f} %)")
    print(f"Top {args.show} matching series by number of votes:")
    (
        result.orderBy(F.desc_nulls_last("series_votes"))
        .select("series_title", "startYear", "endYear",
                "qualifying_seasons",
                "last_season", "best_season", "best_rating",
                "worst_season", "worst_rating", "series_votes")
        .show(args.show, truncate=40)
    )
    spark.stop()


if __name__ == "__main__":
    main()