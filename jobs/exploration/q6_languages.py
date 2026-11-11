"""Q6: actors and actresses who succeeded in films of several languages.

Two Spark steps around a Wikidata lookup done outside Spark:

1. --step export   : select the successful movies and write their IDs.
2. (outside Spark)  : q6_fetch_languages.py gets the original language(s)
                      of each movie from Wikidata (property P364).
3. --step analyze  : count, for each actor or actress, the distinct
                      original languages of their successful movies.

Definitions (all thresholds are parameters):
- a successful movie is a title of type movie with an average rating of
  at least --min-rating and at least --min-votes votes;
- only the actor and actress categories of title.principals are used;
- by default a movie listing several original languages is left out,
  because one multilingual film would otherwise give an actor several
  languages at once (--include-multilingual keeps them);
- by default every film counted has exactly one language, so each
  language of an actor comes from a different film. With
  --include-multilingual this no longer holds (one film can bring several
  languages), so two counts are printed: all languages (an upper bound)
  and only the languages found in single-language films (a lower bound);
- Wikidata sometimes names the same language in several ways (a variety
  such as "American English", or a synonym such as "Yue Chinese" for
  Cantonese). LANGUAGE_ALIASES maps these labels to one language, and
  LANGUAGE_EXCLUDED removes values that are not a natural language (an
  invented film language, no spoken dialogue). The step prints every
  language label it keeps, so the list can be checked.

Usage (inside the pyspark_notebook container, from /home/jovyan):
    spark-submit --driver-memory 4g jobs/exploration/q6_languages.py \
        --step export
    python -u jobs/exploration/q6_fetch_languages.py --contact you@x.fr
    spark-submit --driver-memory 4g jobs/exploration/q6_languages.py \
        --step analyze
"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (ArrayType, StringType, StructField,
                               StructType)

IMDB_NULL = "\\N"

# Varieties and synonyms mapped to one language. Chinese labels follow the
# usual split between Mandarin (standard Chinese) and Cantonese.
LANGUAGE_ALIASES = {
    "American English": "English",
    "British English": "English",
    "Australian English": "English",
    "Canadian English": "English",
    "Indian English": "English",
    "Canadian French": "French",
    "Quebec French": "French",
    "Belgian French": "French",
    "Swiss German": "German",
    "Austrian German": "German",
    "Mexican Spanish": "Spanish",
    "Latin American Spanish": "Spanish",
    "Rioplatense Spanish": "Spanish",
    "European Spanish": "Spanish",
    "Brazilian Portuguese": "Portuguese",
    "European Portuguese": "Portuguese",
    "Yue Chinese": "Cantonese",
    "Standard Cantonese": "Cantonese",
    "Putonghua": "Mandarin",
    "Standard Mandarin": "Mandarin",
    "Mandarin Chinese": "Mandarin",
    "Standard Chinese": "Mandarin",
    "Standard Taiwanese Mandarin": "Mandarin",
    "Modern Greek": "Greek",
    "Flemish": "Dutch",
    "Belgian Dutch": "Dutch",
    "Kurdish language": "Kurdish",
    "Bokmål": "Norwegian",
    "Norwegian Bokmål": "Norwegian",
    "Nynorsk": "Norwegian",
}
# Values that are not a natural language, and the bare label "Chinese",
# which does not say whether the film is in Mandarin or in Cantonese.
LANGUAGE_EXCLUDED = [
    "artistic language", "constructed language", "fictional language",
    "no linguistic content", "no value", "Chinese",
]

LANGUAGE_SCHEMA = StructType([
    StructField("imdb_id", StringType(), False),
    StructField("wikidata_uri", StringType(), True),
    StructField("original_languages", ArrayType(StringType()), True),
    StructField("status", StringType(), True),
    StructField("source_url", StringType(), True),
    StructField("retrieved_at", StringType(), True),
])


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


def successful_movies(spark, args):
    basics = read_raw(spark, args.raw_dir, "title_basics",
                      ["tconst", "titleType", "primaryTitle", "startYear"])
    ratings = read_raw(spark, args.raw_dir, "title_ratings",
                       ["tconst", "averageRating", "numVotes"])
    return (
        basics.filter(F.col("titleType") == "movie")
        .join(ratings, on="tconst", how="inner")
        .withColumn("averageRating", safe_cast("averageRating", "DOUBLE"))
        .withColumn("numVotes", safe_cast("numVotes", "BIGINT"))
        .withColumn("startYear", safe_cast("startYear", "INT"))
        .filter((F.col("averageRating") >= args.min_rating)
                & (F.col("numVotes") >= args.min_votes))
        .select("tconst", "primaryTitle", "startYear", "averageRating",
                "numVotes")
    )


def step_export(spark, args):
    movies = successful_movies(spark, args)
    movies.write.mode("overwrite").parquet(f"{args.out_dir}/movies")
    movies = spark.read.parquet(f"{args.out_dir}/movies")
    movies.select("tconst").coalesce(1).write.mode("overwrite") \
        .option("header", True).csv(f"{args.out_dir}/ids_movies")
    print(f"Successful movies (rating >= {args.min_rating}, "
          f"votes >= {args.min_votes}): {movies.count()}")


def normalize_languages(df):
    """Map language varieties and synonyms to one label, drop non-languages.

    Two labels of the same film can collapse to one language, so the
    result is deduplicated per film.
    """
    aliases = F.create_map(*[
        F.lit(x) for pair in LANGUAGE_ALIASES.items() for x in pair])
    return (
        df.withColumn("language",
                      F.coalesce(aliases[F.col("language")],
                                 F.col("language")))
        .filter(~F.col("language").isin(LANGUAGE_EXCLUDED))
        .dropDuplicates(["tconst", "language"])
    )


def step_analyze(spark, args):
    movies = spark.read.parquet(f"{args.out_dir}/movies")
    languages = spark.read.schema(LANGUAGE_SCHEMA) \
        .json(f"{args.out_dir}/languages.jsonl") \
        .dropDuplicates(["imdb_id"]) \
        .withColumnRenamed("imdb_id", "tconst")

    # Normalize the labels first, so that a film listed as "English" and
    # "American English" counts as a single-language film.
    normalized = normalize_languages(
        languages.select(
            "tconst",
            F.explode("original_languages").alias("language"))
    ).groupBy("tconst").agg(
        F.array_sort(F.collect_set("language")).alias("languages"))

    movies = movies.join(normalized, on="tconst", how="left") \
        .withColumn("n_languages",
                    F.coalesce(F.size("languages"), F.lit(0)))

    print("Successful movies by number of original languages "
          "(after normalization):")
    movies.groupBy("n_languages").count().orderBy("n_languages").show()

    usable = movies.filter(F.col("n_languages") >= 1)
    if not args.include_multilingual:
        usable = usable.filter(F.col("n_languages") == 1)
    film_language = usable.select(
        "tconst", "primaryTitle", "startYear",
        F.col("n_languages").alias("film_n_languages"),
        F.explode("languages").alias("language"))

    n_usable = usable.count()
    n_english = film_language.filter(F.col("language") == "English") \
        .select("tconst").distinct().count()
    print(f"Films used: {n_usable}, in English: {n_english} "
          f"({100 * n_english / max(n_usable, 1):.1f} %)")

    print("Language labels used after normalization:")
    film_language.groupBy("language").agg(
        F.countDistinct("tconst").alias("films")
    ).orderBy(F.desc("films")).show(60, truncate=False)

    principals = read_raw(spark, args.raw_dir, "title_principals",
                          ["tconst", "nconst", "category"])
    names = read_raw(spark, args.raw_dir, "name_basics",
                     ["nconst", "primaryName"])
    actors = principals.filter(
        F.col("category").isin("actor", "actress")
    ).select("tconst", "nconst").distinct()

    per_actor = (
        actors.join(film_language, on="tconst", how="inner")
        .groupBy("nconst")
        .agg(
            F.countDistinct("language").alias("n_languages"),
            F.countDistinct(
                F.when(F.col("film_n_languages") == 1, F.col("language"))
            ).alias("n_languages_single"),
            F.countDistinct("tconst").alias("n_films"),
            F.array_sort(F.collect_set("language")).alias("languages"),
        )
        .filter(F.col("n_languages") >= 2)
        .join(names, on="nconst", how="left")
    )
    per_actor.write.mode("overwrite").parquet(f"{args.out_dir}/actors")
    per_actor = spark.read.parquet(f"{args.out_dir}/actors")

    if args.include_multilingual:
        for k in (2, 3):
            lower = per_actor.filter(
                F.col("n_languages_single") >= k).count()
            print(f">= {k} languages from single-language films only "
                  f"(lower bound): {lower}")

    at_least_three = per_actor.filter(F.col("n_languages") >= 3)
    print(f"Actors/actresses with successful films in >= 2 languages: "
          f"{per_actor.count()}")
    print(f"Actors/actresses with successful films in >= 3 languages: "
          f"{at_least_three.count()}")
    print("Most frequent language pairs/sets (>= 2 languages):")
    per_actor.groupBy("languages").count() \
        .orderBy(F.desc("count")).show(10, truncate=60)
    print("Actors/actresses with >= 3 languages:")
    at_least_three.orderBy(F.desc("n_languages"), F.desc("n_films")) \
        .select("primaryName", "n_languages", "n_films", "languages") \
        .show(args.show, truncate=70)


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--step", choices=["export", "analyze"],
                        required=True)
    parser.add_argument("--raw-dir", default="data/imdb/raw")
    parser.add_argument("--out-dir", default="data/exploration/q6")
    parser.add_argument("--min-rating", type=float, default=7.0)
    parser.add_argument("--min-votes", type=int, default=10000)
    parser.add_argument("--include-multilingual", action="store_true")
    parser.add_argument("--show", type=int, default=30)
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName(f"q6_languages_{args.step}")
        .config("spark.sql.shuffle.partitions", "64")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    if args.step == "export":
        step_export(spark, args)
    else:
        step_analyze(spark, args)
    spark.stop()


if __name__ == "__main__":
    main()