"""Select the films and people to enrich.

For every (genre, year) pair, keep the top N feature-length movies ranked by
number of votes, then keep the first K principals of each selected film.
The input is the raw Parquet layer written by jobs/ingest_imdb.py, where every
column is a string and IMDb nulls are the literal "\\N". All thresholds are
parameters, so the same code runs unchanged on the full dataset.

Usage (inside the pyspark_notebook container, from /home/jovyan):
    spark-submit --driver-memory 4g jobs/enrichment/select_films.py
"""

import argparse

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

IMDB_NULL = "\\N"


def read_raw(spark, raw_dir, table, columns):
    """Read some columns of a raw table and turn IMDb null markers into nulls.

    Values equal to "\\N" or made only of spaces become real nulls, so that
    filters and casts behave as expected.
    """
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


def select_films(basics, ratings, year_min, year_max, min_runtime, top_n):
    """Return the top N movies per (genre, year), ranked by votes."""
    movies = (
        basics
        .filter(F.col("titleType") == "movie")
        .filter(F.col("isAdult") == "0")
        .withColumn("startYear", safe_cast("startYear", "INT"))
        .withColumn("runtimeMinutes", safe_cast("runtimeMinutes", "INT"))
        .filter(F.col("startYear").between(year_min, year_max))
        .filter(F.col("runtimeMinutes") >= min_runtime)
        .filter(F.col("genres").isNotNull())
        .join(
            ratings
            .withColumn("numVotes", safe_cast("numVotes", "BIGINT"))
            .withColumn("averageRating", safe_cast("averageRating", "DOUBLE"))
            .filter(F.col("numVotes").isNotNull()),
            on="tconst",
            how="inner",
        )
        .withColumn("genre", F.explode(F.split("genres", ",")))
    )

    window = Window.partitionBy("genre", "startYear").orderBy(
        F.col("numVotes").desc(),
        F.col("averageRating").desc(),
        F.col("tconst"),  # deterministic tie-break
    )
    return (
        movies
        .withColumn("rank", F.row_number().over(window))
        .filter(F.col("rank") <= top_n)
        .select("tconst", "primaryTitle", "startYear", "genre", "rank",
                "numVotes", "averageRating", "runtimeMinutes")
    )


def select_principals(principals, film_ids, n_principals):
    """Return the first K principals (by IMDb ordering) of each film."""
    with_order = principals.withColumn("ordering",
                                       safe_cast("ordering", "INT"))
    window = Window.partitionBy("tconst").orderBy(F.col("ordering"))
    return (
        with_order
        .join(film_ids, on="tconst", how="inner")
        .filter(F.col("nconst").isNotNull())
        .withColumn("rn", F.row_number().over(window))
        .filter(F.col("rn") <= n_principals)
        .select("tconst", "nconst", "ordering", "category")
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--raw-dir", default="data/imdb/raw")
    parser.add_argument("--out-dir", default="data/enrichment/selection")
    parser.add_argument("--year-min", type=int, default=1980)
    parser.add_argument("--year-max", type=int, default=2025)
    parser.add_argument("--min-runtime", type=int, default=40)
    parser.add_argument("--top-n", type=int, default=10)
    parser.add_argument("--n-principals", type=int, default=3)
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("enrichment_selection")
        .config("spark.sql.shuffle.partitions", "64")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    basics = read_raw(spark, args.raw_dir, "title_basics",
                      ["tconst", "titleType", "primaryTitle", "isAdult",
                       "startYear", "runtimeMinutes", "genres"])
    ratings = read_raw(spark, args.raw_dir, "title_ratings",
                       ["tconst", "averageRating", "numVotes"])
    principals = read_raw(spark, args.raw_dir, "title_principals",
                          ["tconst", "ordering", "nconst", "category"])

    films = select_films(basics, ratings, args.year_min, args.year_max,
                         args.min_runtime, args.top_n)
    films.write.mode("overwrite").parquet(f"{args.out_dir}/films_top")
    films = spark.read.parquet(f"{args.out_dir}/films_top")

    # A film can rank in several genres: keep each ID once.
    film_ids = films.select("tconst").distinct()
    people = select_principals(principals, film_ids, args.n_principals)
    people.write.mode("overwrite").parquet(f"{args.out_dir}/principals_top")
    people = spark.read.parquet(f"{args.out_dir}/principals_top")

    # Plain ID lists read by the scraper, which runs outside Spark.
    film_ids.coalesce(1).write.mode("overwrite").option("header", True) \
        .csv(f"{args.out_dir}/ids_films")
    people.select("nconst").distinct().coalesce(1).write \
        .mode("overwrite").option("header", True) \
        .csv(f"{args.out_dir}/ids_people")

    print(f"(genre, year, film) rows: {films.count()}")
    print(f"distinct films: {film_ids.count()}")
    print(f"film-principal pairs: {people.count()}")
    print(f"distinct people: {people.select('nconst').distinct().count()}")
    spark.stop()


if __name__ == "__main__":
    main()