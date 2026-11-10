"""Build the IMDb lookup table used to match Wikipedia page titles.

Wikipedia article titles and IMDb names are compared on a normalized key
(lower case, trimmed, without the trailing disambiguation in parentheses,
such as "(2010 film)" or "(actor)"). The lookup contains:

- titles: movies and series with at least --min-votes IMDb votes, keyed by
  their primary title, original title and localized titles (title.akas);
- people: actors, actresses, directors, writers and producers known for a
  title with at least --min-person-votes votes (a higher threshold, since
  IMDb lists millions of crew members and many share common names).

Only reasonably popular entries are kept so that common words ("Home",
"Paris") are less likely to produce false matches. When several entries
share a key, the most voted one wins.

Usage (inside the pyspark_notebook container):
    spark-submit --driver-memory 4g /home/jovyan/jobs/wiki_imdb_lookup.py
"""

import argparse
from pathlib import Path

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

TITLE_TYPES = ["movie", "tvMovie", "tvSeries", "tvMiniSeries"]
PERSON_PROFESSIONS = ["actor", "actress", "director", "writer", "producer"]
# Regions whose localized titles match the Wikipedia editions we stream.
AKA_REGIONS = ["US", "GB", "FR", "DE", "ES", "IT"]


def normalize(column):
    """Return the normalized matching key of a title or a name column."""
    without_suffix = F.regexp_replace(column, r"\s*\([^)]*\)\s*$", "")
    collapsed = F.regexp_replace(without_suffix, r"\s+", " ")
    return F.lower(F.trim(collapsed))


def to_int(column_name):
    """Cast an IMDb string column to int, "\\N" becoming null."""
    return F.expr(f"try_cast({column_name} AS INT)")


def popular_titles(spark, raw_dir, min_votes):
    """Movies and series with enough votes, with their vote count."""
    basics = spark.read.parquet(str(raw_dir / "title_basics"))
    ratings = spark.read.parquet(str(raw_dir / "title_ratings"))
    return (
        basics
        .filter(F.col("titleType").isin(TITLE_TYPES))
        .join(ratings, "tconst")
        .select(
            "tconst",
            "titleType",
            "primaryTitle",
            "originalTitle",
            to_int("startYear").alias("start_year"),
            to_int("numVotes").alias("num_votes"),
        )
        .filter(F.col("num_votes") >= min_votes)
    )


def title_keys(spark, raw_dir, titles):
    """One row per (key, title) from primary, original and local names."""
    akas = (
        spark.read.parquet(str(raw_dir / "title_akas"))
        .filter(F.col("region").isin(AKA_REGIONS))
        .select(F.col("titleId").alias("tconst"),
                F.col("title").alias("name"))
    )
    names = (
        titles.select("tconst", F.col("primaryTitle").alias("name"))
        .unionByName(
            titles.select("tconst", F.col("originalTitle").alias("name")))
        .unionByName(akas.join(titles.select("tconst"), "tconst"))
    )
    return (
        names.join(titles, "tconst")
        .select(
            normalize(F.col("name")).alias("match_key"),
            F.lit("title").alias("entity_type"),
            F.col("tconst").alias("imdb_id"),
            F.col("primaryTitle").alias("imdb_name"),
            F.col("titleType").alias("imdb_category"),
            "start_year",
            "num_votes",
        )
    )


def person_keys(spark, raw_dir, titles, min_person_votes):
    """One row per film professional known for a very popular title."""
    people = spark.read.parquet(str(raw_dir / "name_basics"))
    known_for = (
        people
        .withColumn("tconst",
                    F.explode(F.split(F.col("knownForTitles"), ",")))
        .join(titles.select("tconst", "num_votes"), "tconst")
        .groupBy("nconst", "primaryName", "primaryProfession")
        .agg(F.max("num_votes").alias("num_votes"))
        .withColumn("main_profession",
                    F.split(F.col("primaryProfession"), ",")[0])
        .filter(F.col("main_profession").isin(PERSON_PROFESSIONS))
        .filter(F.col("num_votes") >= min_person_votes)
    )
    return known_for.select(
        normalize(F.col("primaryName")).alias("match_key"),
        F.lit("person").alias("entity_type"),
        F.col("nconst").alias("imdb_id"),
        F.col("primaryName").alias("imdb_name"),
        F.col("main_profession").alias("imdb_category"),
        F.lit(None).cast("int").alias("start_year"),
        "num_votes",
    )


def keep_most_voted(df, keys):
    """Keep the most voted entry for each value of the given keys."""
    order = [F.col("num_votes").desc(), F.col("imdb_id")]
    window = Window.partitionBy(*keys).orderBy(*order)
    return (
        df.withColumn("rank", F.row_number().over(window))
        .filter(F.col("rank") == 1)
        .drop("rank")
    )


def build_lookups(spark, raw_dir, min_votes, min_person_votes):
    """Return (lookup by key, lookup by key and year) DataFrames."""
    titles = popular_titles(spark, raw_dir, min_votes).cache()
    entries = (
        title_keys(spark, raw_dir, titles)
        .unionByName(person_keys(spark, raw_dir, titles, min_person_votes))
        .filter(F.length("match_key") > 0)
        .dropDuplicates(["match_key", "imdb_id"])
    )
    by_key = keep_most_voted(entries, ["match_key"])
    by_key_year = keep_most_voted(
        entries.filter(F.col("start_year").isNotNull()),
        ["match_key", "start_year"],
    )
    return by_key, by_key_year


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--data-dir", default="/home/jovyan/data")
    parser.add_argument(
        "--min-votes",
        type=int,
        default=1000,
        help="Minimum IMDb votes for a title (default: %(default)s)",
    )
    parser.add_argument(
        "--min-person-votes",
        type=int,
        default=20000,
        help="Minimum votes of a person's best known title "
             "(default: %(default)s)",
    )
    return parser.parse_args()


def main():
    """Build both lookup tables and write them as Parquet."""
    args = parse_args()
    data_dir = Path(args.data_dir)
    raw_dir = data_dir / "imdb" / "raw"
    out_dir = data_dir / "imdb" / "wiki_lookup"

    spark = SparkSession.builder.appName("wiki_imdb_lookup").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    by_key, by_key_year = build_lookups(
        spark, raw_dir, args.min_votes, args.min_person_votes)
    by_key.write.mode("overwrite").parquet(str(out_dir / "by_key"))
    by_key_year.write.mode("overwrite").parquet(str(out_dir / "by_key_year"))

    for name in ("by_key", "by_key_year"):
        df = spark.read.parquet(str(out_dir / name))
        print(f"{name}: {df.count()} rows")
        df.groupBy("entity_type").count().show()

    spark.stop()


if __name__ == "__main__":
    main()
