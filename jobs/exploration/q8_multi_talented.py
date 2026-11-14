"""Q8: multi-talented film professionals and how they perform.

Roles considered: director and writer (from title_crew, complete) and
actor (actor or actress in title_principals, which only lists the main
credits of a title). Only feature-length movies (titleType = "movie",
runtime >= 40 minutes) with at least --min-votes votes are used.

A person is multi-talented when they held at least two different roles,
each on at least --min-movies movies over their career.

Performance: for each multi-talented person, the mean rating of the
movies where they held two roles or more is compared with the mean
rating of the movies where they held only one (paired comparison, each
person compared with themselves). A pooled view by role combination is
also given.

Usage (inside the pyspark_notebook container):
    spark-submit --driver-memory 6g \\
        /home/jovyan/jobs/exploration/q8_multi_talented.py
    spark-submit /home/jovyan/jobs/exploration/q8_multi_talented.py \\
        --min-movies 5
"""

import argparse
import logging
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

MIN_RUNTIME = 40

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("q8_multi_talented")


def movie_credits(titles, ratings, crew, principals, min_votes):
    """Return one row per (person, rated feature-length movie, role)."""
    movies = (
        titles
        .filter(
            (F.col("titleType") == "movie")
            & (F.col("runtimeMinutes") >= MIN_RUNTIME)
        )
        .join(ratings.filter(F.col("numVotes") >= min_votes), "tconst")
        .select("tconst", "averageRating", "numVotes")
    )
    directors = crew.select(
        "tconst", F.explode("directors").alias("nconst"),
        F.lit("director").alias("role"))
    writers = crew.select(
        "tconst", F.explode("writers").alias("nconst"),
        F.lit("writer").alias("role"))
    actors = (
        principals
        .filter(F.col("category").isin("actor", "actress"))
        .select("tconst", "nconst", F.lit("actor").alias("role"))
    )
    return (
        directors.unionByName(writers).unionByName(actors)
        .join(movies, "tconst")
        .distinct()
    )


def multi_talented(credits, min_movies):
    """Return the people with two roles or more, each on enough movies."""
    roles = (
        credits.groupBy("nconst", "role")
        .agg(F.countDistinct("tconst").alias("n_movies"))
        .filter(F.col("n_movies") >= min_movies)
    )
    return (
        roles.groupBy("nconst")
        .agg(
            F.sort_array(F.collect_list("role")).alias("roles"),
            F.sum("n_movies").alias("n_credits"),
        )
        .filter(F.size("roles") >= 2)
        .withColumn("career_combination", F.concat_ws("+", "roles"))
        .drop("roles")
    )


def roles_per_movie(credits, talents):
    """One row per (multi-talented person, movie) with the roles held."""
    return (
        credits.join(talents.select("nconst"), "nconst", "left_semi")
        .groupBy("nconst", "tconst", "averageRating", "numVotes")
        .agg(F.sort_array(F.collect_set("role")).alias("movie_roles"))
        .withColumn("n_roles", F.size("movie_roles"))
        .withColumn("movie_combination", F.concat_ws("+", "movie_roles"))
        .drop("movie_roles")
    )


def paired_comparison(per_movie):
    """Compare, for each person, cumulated-role movies with single-role."""
    multi = F.col("n_roles") >= 2
    return (
        per_movie.groupBy("nconst")
        .agg(
            F.sum(F.when(multi, 1).otherwise(0)).alias("n_multi"),
            F.sum(F.when(~multi, 1).otherwise(0)).alias("n_single"),
            F.avg(F.when(multi, F.col("averageRating"))).alias("multi_avg"),
            F.avg(F.when(~multi, F.col("averageRating")))
            .alias("single_avg"),
        )
        .filter((F.col("n_multi") > 0) & (F.col("n_single") > 0))
        .withColumn("diff", F.col("multi_avg") - F.col("single_avg"))
    )


def paired_summary(paired):
    """Summarise the per person differences."""
    return paired.agg(
        F.count("*").alias("n_people"),
        F.round(F.avg("diff"), 3).alias("mean_diff"),
        F.round(F.percentile_approx("diff", 0.5), 3).alias("median_diff"),
        F.round(100.0 * F.avg(F.when(F.col("diff") > 0, 1).otherwise(0)),
                1).alias("pct_better_when_multi"),
    )


def pooled_by_combination(per_movie):
    """Average rating and votes for each combination held on a movie."""
    return (
        per_movie.groupBy("movie_combination")
        .agg(
            F.count("*").alias("n_person_movies"),
            F.round(F.avg("averageRating"), 2).alias("avg_rating"),
            F.percentile_approx("numVotes", 0.5).alias("median_votes"),
        )
        .orderBy(F.desc("n_person_movies"))
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
        "--min-votes",
        type=int,
        default=100,
        help="Minimum votes for a movie to be counted (default: %(default)s)",
    )
    parser.add_argument(
        "--min-movies",
        type=int,
        default=3,
        help="Minimum movies per role to count that role "
             "(default: %(default)s)",
    )
    return parser.parse_args()


def main():
    """Run Q8 and save every result table."""
    args = parse_args()
    data_dir = Path(args.data_dir)
    clean_dir = data_dir / "imdb" / "clean"
    output_dir = (data_dir / "exploration" / "q8"
                  / f"votes_{args.min_votes}_movies_{args.min_movies}")

    spark = (
        SparkSession.builder
        .appName("q8_multi_talented")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    def read(name):
        return spark.read.parquet(str(clean_dir / name))

    credits = movie_credits(
        read("title_basics"), read("title_ratings"), read("title_crew"),
        read("title_principals"), args.min_votes).cache()
    talents = multi_talented(credits, args.min_movies).cache()
    per_movie = roles_per_movie(credits, talents).cache()
    paired = paired_comparison(per_movie).cache()
    names = read("name_basics").select("nconst", "primaryName")

    n_people = credits.select("nconst").distinct().count()
    log.info("%d people credited, %d multi-talented", n_people,
             talents.count())

    combinations = (talents.groupBy("career_combination").count()
                    .orderBy(F.desc("count")))
    print("\n=== Q8 multi-talented people by career combination ===")
    combinations.show(truncate=False)

    print("\n=== Q8 paired comparison (multi-role vs single-role movies) ===")
    summary = paired_summary(paired)
    summary.show(truncate=False)

    print("\n=== Q8 pooled view by role combination on the same movie ===")
    pooled = pooled_by_combination(per_movie)
    pooled.show(truncate=False)

    top = (
        paired.join(talents, "nconst").join(names, "nconst")
        .select(
            "primaryName", "career_combination", "n_multi", "n_single",
            F.round("multi_avg", 2).alias("multi_avg"),
            F.round("single_avg", 2).alias("single_avg"),
            F.round("diff", 2).alias("diff"),
        )
        .orderBy(F.desc("n_multi"))
    )
    print("\n=== Q8 people with the most multi-role movies ===")
    top.show(20, truncate=False)

    save_csv(combinations, output_dir / "career_combinations")
    save_csv(summary, output_dir / "paired_summary")
    save_csv(pooled, output_dir / "pooled_by_combination")
    save_csv(top, output_dir / "people")
    log.info("Results written to %s", output_dir)
    spark.stop()


if __name__ == "__main__":
    main()
