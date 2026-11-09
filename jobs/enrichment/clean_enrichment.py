"""Clean the scraped Wikipedia data and join it to the selected films.

Reads the JSON lines written by wiki_scraper.py with an explicit schema,
extracts a synopsis from each film article (the "Plot" section, or the lead
section when there is none), cleans the biographies, adds a note whenever
information is missing, and joins everything back to the selection made by
select_films.py. Every output row keeps its source URL for lineage.

Usage (inside the pyspark_notebook container, from /home/jovyan):
    spark-submit --driver-memory 4g jobs/enrichment/clean_enrichment.py
"""

import argparse

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (ArrayType, StringType, StructField,
                               StructType)

# Level-2 headings that hold a film's story on English Wikipedia.
PLOT_HEADINGS = "Plot|Plot summary|Synopsis|Summary|Premise|Story|Storyline"
PLOT_PATTERN = (
    rf"(?s)\n== (?:{PLOT_HEADINGS}) ==\n(.*?)(?=\n== [^=]|\Z)"
)
LEAD_PATTERN = r"(?s)^(.*?)(?=\n== [^=]|\Z)"
SUBHEADING_PATTERN = r"\n=+ [^=\n]+ =+\n"

STATUS_NOTES = {
    "no_wikidata_item": "No Wikidata item found for this IMDb ID",
    "no_enwiki_article": "No English Wikipedia article for this item",
    "empty_or_missing_page": "Wikipedia article is missing or empty",
}


def raw_schema(key):
    """Schema of one JSON line written by wiki_scraper.py."""
    return StructType([
        StructField(key, StringType(), False),
        StructField("wikidata_uri", StringType(), True),
        StructField("wikipedia_title", StringType(), True),
        StructField("text", StringType(), True),
        StructField("original_languages", ArrayType(StringType()), True),
        StructField("status", StringType(), True),
        StructField("source_url", StringType(), True),
        StructField("retrieved_at", StringType(), True),
    ])


def read_raw(spark, path, key):
    """Read scraped records and keep the latest one per ID."""
    df = spark.read.schema(raw_schema(key)).json(path)
    latest = Window.partitionBy(key).orderBy(F.col("retrieved_at").desc())
    return (
        df.withColumn("rn", F.row_number().over(latest))
        .filter(F.col("rn") == 1)
        .drop("rn")
        .withColumn("retrieved_at", F.to_timestamp("retrieved_at"))
    )


def clean_text(column):
    """Drop sub-headings, collapse blank lines and spaces, trim."""
    text = F.regexp_replace(column, SUBHEADING_PATTERN, "\n")
    text = F.regexp_replace(text, r"[ \t]+", " ")
    text = F.regexp_replace(text, r"\n{2,}", "\n")
    text = F.trim(text)
    return F.when(F.length(text) > 0, text)


def status_note(status_col):
    """Readable note for a scraping status, null when the status is ok."""
    note = F.lit(None).cast("string")
    for status, message in STATUS_NOTES.items():
        note = F.when(status_col == status, message).otherwise(note)
    return note


def clean_films(films_raw):
    """Extract a synopsis from each film article."""
    plot = clean_text(F.regexp_extract("text", PLOT_PATTERN, 1))
    lead = clean_text(F.regexp_extract("text", LEAD_PATTERN, 1))
    return (
        films_raw
        .withColumn("plot", plot)
        .withColumn("lead", lead)
        .withColumn("synopsis", F.coalesce("plot", "lead"))
        .withColumn(
            "synopsis_type",
            F.when(F.col("plot").isNotNull(), "plot")
            .when(F.col("lead").isNotNull(), "lead")
            .otherwise("none"),
        )
        .withColumn(
            "note",
            F.coalesce(
                status_note(F.col("status")),
                F.when(F.col("synopsis_type") == "lead",
                       "No plot section, lead section used as synopsis"),
                F.when(F.col("synopsis_type") == "none",
                       "No usable text in the Wikipedia article"),
            ),
        )
        .select("tconst", "synopsis", "synopsis_type", "original_languages",
                "wikipedia_title", "source_url", "wikidata_uri",
                "retrieved_at", "note")
    )


def clean_people(people_raw):
    """Clean the introduction of each person article."""
    return (
        people_raw
        .withColumn("biography", clean_text(F.col("text")))
        .withColumn(
            "note",
            F.coalesce(
                status_note(F.col("status")),
                F.when(F.col("biography").isNull(),
                       "No usable text in the Wikipedia article"),
            ),
        )
        .select("nconst", "biography", "wikipedia_title", "source_url",
                "wikidata_uri", "retrieved_at", "note")
    )


def with_missing_note(df, check_col, message):
    """Note rows that were selected but never scraped."""
    return df.withColumn(
        "note",
        F.when(F.col(check_col).isNull() & F.col("note").isNull(),
               F.lit(message)).otherwise(F.col("note")),
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--selection-dir", default="data/enrichment/selection")
    parser.add_argument("--raw-dir", default="data/enrichment/raw")
    parser.add_argument("--out-dir", default="data/enrichment/final")
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("enrichment_clean")
        .config("spark.sql.shuffle.partitions", "16")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    films_top = spark.read.parquet(f"{args.selection_dir}/films_top")
    principals_top = spark.read.parquet(
        f"{args.selection_dir}/principals_top")
    films = clean_films(
        read_raw(spark, f"{args.raw_dir}/films_raw.jsonl", "tconst"))
    people = clean_people(
        read_raw(spark, f"{args.raw_dir}/people_raw.jsonl", "nconst"))

    enriched_films = with_missing_note(
        films_top.join(films, on="tconst", how="left"),
        "retrieved_at", "Not scraped yet",
    )
    enriched_people = with_missing_note(
        principals_top.join(people, on="nconst", how="left"),
        "retrieved_at", "Not scraped yet",
    )

    enriched_films.write.mode("overwrite").parquet(
        f"{args.out_dir}/films_enriched")
    enriched_people.write.mode("overwrite").parquet(
        f"{args.out_dir}/principals_enriched")

    enriched_films = spark.read.parquet(f"{args.out_dir}/films_enriched")
    enriched_people = spark.read.parquet(
        f"{args.out_dir}/principals_enriched")

    print("Films by synopsis type (one row per genre, year, film):")
    enriched_films.groupBy("synopsis_type").count() \
        .orderBy(F.desc("count")).show(truncate=False)
    print("Film notes:")
    enriched_films.groupBy("note").count() \
        .orderBy(F.desc("count")).show(truncate=False)
    print("Principal notes:")
    enriched_people.groupBy("note").count() \
        .orderBy(F.desc("count")).show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()