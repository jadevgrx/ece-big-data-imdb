"""Open question: are reunions with a regular actor better rated?

Question: when a director casts again an actor they have already worked
with at least twice, is the movie better rated than the director's other
movies?

Data: title_basics, title_ratings, title_crew (directors) and
title_principals (actor and actress credits) from the clean layer, no
external data. Only feature-length movies (titleType = "movie", runtime
>= 40 minutes) with a known release year and at least --min-votes votes.

Method:
    - every (director, actor, movie) triple is built; a director acting
      in their own movie is ignored,
    - for each movie, the earlier collaborations of the pair are counted
      over strictly earlier years (movies released the same year cannot
      be ordered),
    - a movie is "with a regular" when at least one actor had
      --min-prior earlier collaborations with the director,
    - each director is compared with themselves: mean rating of the
      movies with a regular against the mean rating of the others
      (paired comparison, as in Q8).

A reunion is only possible once a director has made --min-prior movies,
so movies with a regular come later in a career. The controlled
comparison keeps only the movies made when a reunion was already
possible, which removes the debut movies from the "without" group.
--adjust-year replaces the rating by its difference with the mean rating
of the same year, to remove the era effect.

Usage (inside the pyspark_notebook container):
    spark-submit --driver-memory 6g \\
        --conf "spark.driver.extraJavaOptions=-XX:MaxDirectMemorySize=2g" \\
        /home/jovyan/jobs/open_question/director_actor_duos.py
    spark-submit /home/jovyan/jobs/open_question/director_actor_duos.py \\
        --min-votes 1000 --adjust-year
"""

import argparse
import logging
from pathlib import Path

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

MIN_RUNTIME = 40

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("director_actor_duos")


def rated_movies(titles, ratings, min_votes, adjust_year):
    """Return the rated feature-length movies with the score to compare."""
    movies = (
        titles
        .filter(
            (F.col("titleType") == "movie")
            & (F.col("runtimeMinutes") >= MIN_RUNTIME)
            & F.col("startYear").isNotNull()
            & ~F.col("is_future_release")
        )
        .join(ratings.filter(F.col("numVotes") >= min_votes), "tconst")
        .select("tconst", "startYear", "averageRating", "numVotes")
    )
    if not adjust_year:
        return movies.withColumn("score", F.col("averageRating"))
    yearly = movies.groupBy("startYear").agg(
        F.avg("averageRating").alias("year_avg"))
    return (
        movies.join(yearly, "startYear")
        .withColumn("score", F.col("averageRating") - F.col("year_avg"))
        .drop("year_avg")
    )


def director_movies(movies, crew):
    """Return one row per (director, movie)."""
    return (
        crew.select("tconst", F.explode("directors").alias("director"))
        .join(movies, "tconst")
    )


def collaborations(directed, principals):
    """Return one row per (director, actor, movie) with earlier work.

    prior counts the movies the pair made together in strictly earlier
    years.
    """
    actors = (
        principals
        .filter(F.col("category").isin("actor", "actress"))
        .select("tconst", F.col("nconst").alias("actor"))
        .distinct()
    )
    earlier = (
        Window.partitionBy("director", "actor")
        .orderBy("startYear")
        .rangeBetween(Window.unboundedPreceding, -1)
    )
    return (
        directed.join(actors, "tconst")
        .filter(F.col("director") != F.col("actor"))
        .withColumn("prior", F.count(F.lit(1)).over(earlier))
    )


def classify_movies(directed, pairs, min_prior):
    """Flag each (director, movie) as with or without a regular actor."""
    reunions = pairs.groupBy("director", "tconst").agg(
        F.max("prior").alias("max_prior"))
    career = (
        Window.partitionBy("director")
        .orderBy("startYear")
        .rangeBetween(Window.unboundedPreceding, -1)
    )
    return (
        directed.join(reunions, ["director", "tconst"])
        .withColumn("prior_movies", F.count(F.lit(1)).over(career))
        .withColumn("with_regular", F.col("max_prior") >= min_prior)
        .withColumn("reunion_possible",
                    F.col("prior_movies") >= min_prior)
    )


def paired_comparison(films):
    """Compare, for each director, movies with and without a regular."""
    with_regular = F.col("with_regular")
    return (
        films.groupBy("director")
        .agg(
            F.sum(F.when(with_regular, 1).otherwise(0)).alias("n_with"),
            F.sum(F.when(~with_regular, 1).otherwise(0)).alias("n_without"),
            F.avg(F.when(with_regular, F.col("score"))).alias("with_avg"),
            F.avg(F.when(~with_regular, F.col("score")))
            .alias("without_avg"),
        )
        .filter((F.col("n_with") > 0) & (F.col("n_without") > 0))
        .withColumn("diff", F.col("with_avg") - F.col("without_avg"))
    )


def summarise(paired, label):
    """Summarise the per director differences."""
    return (
        paired.agg(
            F.count("*").alias("n_directors"),
            F.sum("n_with").alias("movies_with"),
            F.sum("n_without").alias("movies_without"),
            F.round(F.avg("diff"), 3).alias("mean_diff"),
            F.round(F.percentile_approx("diff", 0.5), 3)
            .alias("median_diff"),
            F.round(100.0 * F.avg(
                F.when(F.col("diff") > 0, 1).otherwise(0)), 1)
            .alias("pct_better_with_regular"),
        )
        .withColumn("comparison", F.lit(label))
        .select("comparison", "n_directors", "movies_with",
                "movies_without", "mean_diff", "median_diff",
                "pct_better_with_regular")
    )


def loyalty_levels(films):
    """Pooled view by the highest number of earlier collaborations."""
    level = (
        F.when(F.col("max_prior") >= 5, "5+")
        .otherwise(F.col("max_prior").cast("string"))
    )
    return (
        films.withColumn("earlier_collaborations", level)
        .groupBy("earlier_collaborations")
        .agg(
            F.count("*").alias("n_director_movies"),
            F.round(F.avg("averageRating"), 2).alias("avg_rating"),
            F.round(F.avg("score"), 3).alias("avg_score"),
            F.percentile_approx("numVotes", 0.5).alias("median_votes"),
        )
        .orderBy("earlier_collaborations")
    )


def recurrent_duos(pairs, names, min_prior):
    """List the pairs who made at least min_prior + 1 movies together."""
    duos = (
        pairs.groupBy("director", "actor")
        .agg(
            F.count("*").alias("n_movies"),
            F.round(F.avg("averageRating"), 2).alias("avg_rating"),
            F.min("startYear").alias("first_year"),
            F.max("startYear").alias("last_year"),
        )
        .filter(F.col("n_movies") >= min_prior + 1)
    )
    directors = names.select(F.col("nconst").alias("director"),
                             F.col("primaryName").alias("director_name"))
    actors = names.select(F.col("nconst").alias("actor"),
                          F.col("primaryName").alias("actor_name"))
    return (
        duos.join(directors, "director").join(actors, "actor")
        .select("director_name", "actor_name", "n_movies", "avg_rating",
                "first_year", "last_year")
        .orderBy(F.desc("n_movies"), F.desc("avg_rating"))
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
        "--min-prior",
        type=int,
        default=2,
        help="Earlier collaborations needed to be a regular "
             "(default: %(default)s)",
    )
    parser.add_argument(
        "--adjust-year",
        action="store_true",
        help="Compare ratings minus the mean rating of the same year",
    )
    return parser.parse_args()


def main():
    """Run the open question and save every result table."""
    args = parse_args()
    data_dir = Path(args.data_dir)
    clean_dir = data_dir / "imdb" / "clean"
    suffix = "_adjusted" if args.adjust_year else ""
    output_dir = (data_dir / "exploration" / "open_question"
                  / f"votes_{args.min_votes}_prior_{args.min_prior}{suffix}")

    spark = (
        SparkSession.builder
        .appName("director_actor_duos")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    def read(name):
        return spark.read.parquet(str(clean_dir / name))

    movies = rated_movies(read("title_basics"), read("title_ratings"),
                          args.min_votes, args.adjust_year)
    directed = director_movies(movies, read("title_crew")).cache()
    pairs = collaborations(directed, read("title_principals")).cache()
    films = classify_movies(directed, pairs, args.min_prior).cache()
    names = read("name_basics").select("nconst", "primaryName")

    n_films = films.count()
    n_with = films.filter(F.col("with_regular")).count()
    log.info("%d (director, movie) rows, %d with a regular (%.1f %%)",
             n_films, n_with, 100.0 * n_with / n_films)

    naive = paired_comparison(films).cache()
    controlled = paired_comparison(
        films.filter(F.col("reunion_possible"))).cache()
    summary = summarise(naive, "all movies").unionByName(
        summarise(controlled, "reunion possible only"))
    print("\n=== Open question: paired comparison per director ===")
    summary.show(truncate=False)

    print("\n=== Pooled view by earlier collaborations ===")
    loyalty = loyalty_levels(films)
    loyalty.show(truncate=False)

    print("\n=== Most frequent director-actor duos ===")
    duos = recurrent_duos(pairs, names, args.min_prior)
    duos.show(20, truncate=False)

    directors = (
        controlled.join(
            names.withColumnRenamed("nconst", "director"), "director")
        .select(
            "primaryName", "n_with", "n_without",
            F.round("with_avg", 2).alias("with_avg"),
            F.round("without_avg", 2).alias("without_avg"),
            F.round("diff", 2).alias("diff"),
        )
        .orderBy(F.desc("n_with"))
    )
    print("\n=== Directors with the most movies with a regular "
          "(controlled) ===")
    directors.show(20, truncate=False)

    save_csv(summary, output_dir / "summary")
    save_csv(loyalty, output_dir / "loyalty_levels")
    save_csv(duos, output_dir / "duos")
    save_csv(directors, output_dir / "directors")
    log.info("Results written to %s", output_dir)
    spark.stop()


if __name__ == "__main__":
    main()