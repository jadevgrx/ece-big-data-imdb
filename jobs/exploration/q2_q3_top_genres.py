"""Q2 and Q3: top three movie genres per year and how they compare.

Q2: for each year between 1980 and 2025, rank the genres of feature-length
movies (titleType = "movie", runtime >= 40 minutes) by popularity and keep
the top three. Popularity is the total number of IMDb votes (numVotes) of
the movies of the genre released that year. The alternative ranking by
number of movies is available with --metric movies.

Q3: each year, compare the top three genres with all the other genres:
share of the votes, share of the movies, average votes per movie and
vote-weighted average rating. A movie with several genres counts in each
of them, so shares are computed on genre level totals.

Usage (inside the pyspark_notebook container):
    spark-submit --driver-memory 4g /home/jovyan/jobs/exploration/q2_q3_top_genres.py
    spark-submit /home/jovyan/jobs/exploration/q2_q3_top_genres.py --metric movies
"""

import argparse
import logging
from pathlib import Path

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

MIN_RUNTIME = 40
FIRST_YEAR = 1980
LAST_YEAR = 2025
TOP_N = 3
METRIC_COLUMNS = {"votes": "total_votes", "movies": "n_movies"}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("q2_q3_top_genres")


def rated_movies(titles, ratings):
    """Return one row per (rated feature-length movie, genre)."""
    return (
        titles
        .filter(
            (F.col("titleType") == "movie")
            & (F.col("runtimeMinutes") >= MIN_RUNTIME)
            & F.col("startYear").between(FIRST_YEAR, LAST_YEAR)
            & F.col("genres").isNotNull()
        )
        .join(ratings, "tconst")
        .select(
            "tconst",
            F.col("startYear").alias("year"),
            "numVotes",
            "averageRating",
            F.explode("genres").alias("genre"),
        )
    )


def genre_year_stats(movies, metric):
    """Aggregate each (year, genre) and rank the genres inside each year."""
    stats = movies.groupBy("year", "genre").agg(
        F.count("*").alias("n_movies"),
        F.sum("numVotes").alias("total_votes"),
        F.sum(F.col("averageRating") * F.col("numVotes"))
        .alias("rating_x_votes"),
    )
    order = METRIC_COLUMNS[metric]
    window = Window.partitionBy("year").orderBy(
        F.desc(order), F.desc("total_votes"), F.asc("genre"))
    return stats.withColumn("rank", F.row_number().over(window))


def top_genres_per_year(stats):
    """Q2: one row per year with its top genres in order."""
    top = stats.filter(F.col("rank") <= TOP_N)
    return (
        top.groupBy("year")
        .agg(F.sort_array(F.collect_list(
            F.struct("rank", "genre", "total_votes", "n_movies")))
            .alias("top"))
        .select(
            "year",
            *[F.col("top")[i]["genre"].alias(f"genre_{i + 1}")
              for i in range(TOP_N)],
            *[F.col("top")[i]["total_votes"].alias(f"votes_{i + 1}")
              for i in range(TOP_N)],
        )
        .orderBy("year")
    )


def compare_top_with_rest(stats):
    """Q3: compare the top genres of each year with the other genres."""
    grouped = (
        stats
        .withColumn(
            "group",
            F.when(F.col("rank") <= TOP_N, "top").otherwise("rest"),
        )
        .groupBy("year", "group")
        .agg(
            F.count("*").alias("genres"),
            F.sum("n_movies").alias("movies"),
            F.sum("total_votes").alias("votes"),
            F.sum("rating_x_votes").alias("rxv"),
        )
    )
    wide = grouped.groupBy("year").pivot("group", ["top", "rest"]).agg(
        F.first("genres").alias("genres"),
        F.first("movies").alias("movies"),
        F.first("votes").alias("votes"),
        F.first("rxv").alias("rxv"),
    )
    return (
        wide.select(
            "year",
            F.col("rest_genres").alias("n_other_genres"),
            F.round(100.0 * F.col("top_votes")
                    / (F.col("top_votes") + F.col("rest_votes")), 1)
            .alias("top_vote_share_pct"),
            F.round(100.0 * F.col("top_movies")
                    / (F.col("top_movies") + F.col("rest_movies")), 1)
            .alias("top_movie_share_pct"),
            F.round(F.col("top_votes") / F.col("top_movies"), 0)
            .alias("top_votes_per_movie"),
            F.round(F.col("rest_votes") / F.col("rest_movies"), 0)
            .alias("rest_votes_per_movie"),
            F.round(F.col("top_rxv") / F.col("top_votes"), 2)
            .alias("top_avg_rating"),
            F.round(F.col("rest_rxv") / F.col("rest_votes"), 2)
            .alias("rest_avg_rating"),
        )
        .orderBy("year")
    )


def genre_frequency(stats):
    """How many years each genre reached the top three and first place."""
    top = stats.filter(F.col("rank") <= TOP_N)
    return (
        top.groupBy("genre")
        .agg(
            F.count("*").alias("years_in_top"),
            F.sum(F.when(F.col("rank") == 1, 1).otherwise(0))
            .alias("years_first"),
            F.min("year").alias("first_year"),
            F.max("year").alias("last_year"),
        )
        .orderBy(F.desc("years_in_top"), F.desc("years_first"))
    )


def decade_summary(comparison):
    """Average the Q3 indicators by decade to show the trend."""
    return (
        comparison
        .withColumn("decade", (F.floor(F.col("year") / 10) * 10).cast("int"))
        .groupBy("decade")
        .agg(
            F.round(F.avg("top_vote_share_pct"), 1)
            .alias("avg_top_vote_share_pct"),
            F.round(F.avg("top_movie_share_pct"), 1)
            .alias("avg_top_movie_share_pct"),
            F.round(F.avg("top_avg_rating"), 2).alias("avg_top_rating"),
            F.round(F.avg("rest_avg_rating"), 2).alias("avg_rest_rating"),
        )
        .orderBy("decade")
    )


def save_csv(df, path):
    """Write a small result as a single CSV file with a header."""
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(
        str(path))


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--data-dir",
        default="/home/jovyan/data",
        help="Root data directory (default: %(default)s)",
    )
    parser.add_argument(
        "--metric",
        choices=list(METRIC_COLUMNS),
        default="votes",
        help="Popularity used to rank genres (default: %(default)s)",
    )
    return parser.parse_args()


def main():
    """Run Q2 and Q3 and save every result table."""
    args = parse_args()
    data_dir = Path(args.data_dir)
    clean_dir = data_dir / "imdb" / "clean"
    output_dir = data_dir / "exploration" / "q2_q3" / args.metric

    spark = (
        SparkSession.builder
        .appName("q2_q3_top_genres")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    titles = spark.read.parquet(str(clean_dir / "title_basics"))
    ratings = spark.read.parquet(str(clean_dir / "title_ratings"))
    stats = genre_year_stats(rated_movies(titles, ratings),
                             args.metric).cache()

    top = top_genres_per_year(stats)
    comparison = compare_top_with_rest(stats).cache()
    frequency = genre_frequency(stats)
    decades = decade_summary(comparison)

    print(f"\n=== Q2 top {TOP_N} genres per year (metric: {args.metric}) ===")
    top.show(50, truncate=False)
    print("\n=== Q3 top genres compared with the other genres ===")
    comparison.show(50, truncate=False)
    print("\n=== Q3 how often each genre is in the top ===")
    frequency.show(30, truncate=False)
    print("\n=== Q3 trend by decade ===")
    decades.show(truncate=False)

    save_csv(top, output_dir / "q2_top_genres")
    save_csv(comparison, output_dir / "q3_comparison")
    save_csv(frequency, output_dir / "q3_genre_frequency")
    save_csv(decades, output_dir / "q3_decades")
    log.info("Results written to %s", output_dir)
    spark.stop()


if __name__ == "__main__":
    main()