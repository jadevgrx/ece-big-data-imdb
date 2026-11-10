"""Q5: series whose worst episode is in the last season and best episode
in the first two seasons.

Definitions (all thresholds are parameters):
- a series is a title of type tvSeries or tvMiniSeries;
- an episode counts only if it has a rating with at least --min-votes votes;
- a qualifying season has at least --min-episodes counted episodes, and a
  series must have at least --min-seasons qualifying seasons;
- by default the threshold only decides which series are eligible: all
  their numbered seasons are then compared, so the real last season and
  the real first two seasons are used even when they are short (a short
  final season, as in Game of Thrones, is often the worst one). With
  --compare-qualifying-only, only qualifying seasons are compared and
  ranked, which was the first version of this script;
- season 0 (specials) is never used;
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
        .filter(F.col("averageRating").isNotNull()
                & (F.col("numVotes") >= min_votes))
    )


def season_sizes(episodes):
    """Number of counted episodes in each season of each series."""
    return (
        episodes.groupBy("parentTconst", "seasonNumber")
        .agg(F.count("*").alias("episodes_in_season"))
    )


def eligible_series(seasons, min_episodes, min_seasons):
    """Series with at least min_seasons seasons of min_episodes episodes."""
    return (
        seasons.filter(F.col("episodes_in_season") >= min_episodes)
        .groupBy("parentTconst")
        .agg(F.count("*").alias("qualifying_seasons"))
        .filter(F.col("qualifying_seasons") >= min_seasons)
    )


def rank_seasons(seasons):
    """Rank each season of a series from the first (1) and the last (1)."""
    by_series = Window.partitionBy("parentTconst")
    return (
        seasons
        .withColumn("rank_from_first",
                    F.row_number().over(by_series.orderBy("seasonNumber")))
        .withColumn("rank_from_last",
                    F.row_number().over(
                        by_series.orderBy(F.desc("seasonNumber"))))
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
    parser.add_argument("--min-votes", type=int, default=100)
    parser.add_argument("--show", type=int, default=25)
    parser.add_argument("--compare-qualifying-only", action="store_true",
                        help="compare only seasons that pass --min-episodes")
    parser.add_argument("--inspect", default=None,
                        help="also print the best and worst episode of the "
                        "candidate series with this exact title")
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
    sizes = season_sizes(rated)
    eligible = eligible_series(sizes, args.min_episodes, args.min_seasons)
    compared = sizes.join(eligible.select("parentTconst"),
                          on="parentTconst", how="inner")
    if args.compare_qualifying_only:
        compared = compared.filter(
            F.col("episodes_in_season") >= args.min_episodes)
    seasons = rank_seasons(compared)

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
    n_seasons = eligible.join(
        seasons.groupBy("parentTconst").agg(
            F.max("seasonNumber").alias("last_season")),
        on="parentTconst", how="inner")

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
    if args.inspect:
        print(f"Candidate series titled {args.inspect!r}:")
        (
            candidates.filter(F.col("series_title") == args.inspect)
            .select("series_title", "startYear", "last_season",
                    "best_season", "best_rating", "worst_season",
                    "worst_rating", "series_votes")
            .show(truncate=40)
        )
    spark.stop()


if __name__ == "__main__":
    main()