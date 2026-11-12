"""Build the clean IMDb layer from the raw Parquet tables.

Every "\\N" marker becomes a real null, numbers get proper types and comma
separated lists become arrays. Data quality issues found by
quality_checks.py are handled with three rules:
    - orphan rows in link tables are dropped, orphan ids are removed
      from id lists (their parent record does not exist),
    - impossible values are set to null (runtime <= 0, death before
      birth, lifespan over 120 years),
    - doubtful but possible values are kept and flagged (extreme
      runtime, release in the future, credit before birth).

Tables are written to data/imdb/clean/<table>. title_basics and
name_basics are written first because the other tables are checked
against them.

Usage (inside the pyspark_notebook container):
    spark-submit --driver-memory 4g \\
        --conf "spark.driver.extraJavaOptions=-XX:MaxDirectMemorySize=2g" \\
        /home/jovyan/jobs/preparation/clean_imdb.py
"""

import argparse
import logging
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

CURRENT_YEAR = 2026
MAX_LIFESPAN = 120
MAX_RUNTIME = 1000
IMDB_NULL_MARKER = "\\N"
TABLE_ORDER = [
    "title_basics",
    "name_basics",
    "title_ratings",
    "title_crew",
    "title_episode",
    "title_principals",
    "title_akas",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("clean_imdb")


def nullify(df):
    """Replace the IMDb null marker and empty strings by real nulls."""
    return df.select([
        F.when(
            (F.col(c) == IMDB_NULL_MARKER) | (F.trim(F.col(c)) == ""), None
        ).otherwise(F.col(c)).alias(c)
        for c in df.columns
    ])


def as_int(column):
    """Cast a string column to int, invalid values become null."""
    return F.col(column).try_cast("int").alias(column)


def as_list(column):
    """Split a comma separated string column into an array."""
    return F.split(F.col(column), ",").alias(column)


def flag(condition):
    """Turn a condition into a boolean that is never null."""
    return F.coalesce(condition, F.lit(False))


def keep_known_ids(df, key, column, reference_ids):
    """Remove from an array column the ids absent from reference_ids.

    The original order of the ids is preserved. A null list stays null,
    a list whose ids are all unknown becomes an empty array.
    """
    reference = reference_ids.toDF("id")
    kept = (
        df.select(key, F.posexplode(column).alias("pos", "id"))
        .join(reference, "id", "left_semi")
        .groupBy(key)
        .agg(F.sort_array(F.collect_list(F.struct("pos", "id")))
             .alias("items"))
        .select(key, F.col("items.id").alias("kept"))
    )
    return (
        df.join(kept, key, "left")
        .withColumn(
            column,
            F.when(F.col(column).isNull(), None).otherwise(
                F.coalesce(F.col("kept"), F.array().cast("array<string>"))
            ),
        )
        .drop("kept")
    )


def clean_title_basics(raw, clean):
    """Type the titles, null invalid runtimes, flag doubtful values."""
    df = nullify(raw)
    runtime = F.col("runtimeMinutes").try_cast("int")
    start = F.col("startYear").try_cast("int")
    return df.select(
        "tconst",
        "titleType",
        "primaryTitle",
        "originalTitle",
        as_int("isAdult"),
        start.alias("startYear"),
        as_int("endYear"),
        F.when(runtime > 0, runtime).alias("runtimeMinutes"),
        as_list("genres"),
        flag(runtime > MAX_RUNTIME).alias("is_extreme_runtime"),
        flag(start > CURRENT_YEAR).alias("is_future_release"),
    )


def clean_name_basics(raw, clean):
    """Type the people, null impossible lifespans, drop unknown titles."""
    df = nullify(raw)
    birth = F.col("birthYear").try_cast("int")
    death = F.col("deathYear").try_cast("int")
    invalid = flag((death < birth) | (death - birth > MAX_LIFESPAN))
    df = df.select(
        "nconst",
        "primaryName",
        F.when(~invalid, birth).alias("birthYear"),
        F.when(~invalid, death).alias("deathYear"),
        as_list("primaryProfession"),
        as_list("knownForTitles"),
        invalid.alias("has_invalid_lifespan"),
    )
    title_ids = clean["title_basics"].select("tconst")
    return keep_known_ids(df, "nconst", "knownForTitles", title_ids)


def clean_title_ratings(raw, clean):
    """Type the ratings and keep only existing titles."""
    df = nullify(raw).select(
        "tconst",
        F.col("averageRating").try_cast("double").alias("averageRating"),
        as_int("numVotes"),
    )
    return df.join(clean["title_basics"].select("tconst"), "tconst",
                   "left_semi")


def clean_title_crew(raw, clean):
    """Keep existing titles and remove unknown directors and writers."""
    df = nullify(raw).select("tconst", as_list("directors"),
                             as_list("writers"))
    df = df.join(clean["title_basics"].select("tconst"), "tconst",
                 "left_semi")
    name_ids = clean["name_basics"].select("nconst")
    df = keep_known_ids(df, "tconst", "directors", name_ids)
    return keep_known_ids(df, "tconst", "writers", name_ids)


def clean_title_episode(raw, clean):
    """Type the episodes and keep those whose episode and series exist."""
    title_ids = clean["title_basics"].select("tconst")
    df = nullify(raw).select(
        "tconst",
        "parentTconst",
        as_int("seasonNumber"),
        as_int("episodeNumber"),
    )
    return (
        df.join(title_ids, "tconst", "left_semi")
        .join(title_ids.withColumnRenamed("tconst", "parentTconst"),
              "parentTconst", "left_semi")
    )


def clean_title_principals(raw, clean):
    """Keep credits whose title and person exist, flag impossible ones.

    The inner joins drop orphan credits (tconst and nconst are unique in
    the clean tables) and bring the years needed for the flag. Archive
    footage is excluded from the flag: appearing in a film made before
    one's birth is normal in that case.
    """
    df = nullify(raw).select(
        "tconst",
        as_int("ordering"),
        "nconst",
        "category",
        "job",
        "characters",
    )
    df = (
        df.join(clean["title_basics"].select("tconst", "startYear"),
                "tconst")
        .join(clean["name_basics"].select("nconst", "birthYear"), "nconst")
    )
    before_birth = (
        (F.col("startYear") < F.col("birthYear"))
        & ~F.col("category").startswith("archive")
    )
    return (
        df.withColumn("is_credit_before_birth", flag(before_birth))
        .drop("startYear", "birthYear")
    )


def clean_title_akas(raw, clean):
    """Type the alternative titles and keep only existing titles."""
    df = nullify(raw).select(
        "titleId",
        as_int("ordering"),
        "title",
        "region",
        "language",
        as_list("types"),
        as_list("attributes"),
        as_int("isOriginalTitle"),
    )
    title_ids = clean["title_basics"].select(
        F.col("tconst").alias("titleId"))
    return df.join(title_ids, "titleId", "left_semi")


CLEANERS = {
    "title_basics": clean_title_basics,
    "name_basics": clean_name_basics,
    "title_ratings": clean_title_ratings,
    "title_crew": clean_title_crew,
    "title_episode": clean_title_episode,
    "title_principals": clean_title_principals,
    "title_akas": clean_title_akas,
}


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--data-dir",
        default="/home/jovyan/data",
        help="Root data directory (default: %(default)s)",
    )
    parser.add_argument(
        "--tables",
        nargs="+",
        choices=TABLE_ORDER,
        default=TABLE_ORDER,
        help="Tables to rebuild (default: all). title_basics and "
             "name_basics must already exist in the clean layer if they "
             "are not rebuilt.",
    )
    return parser.parse_args()


def main():
    """Clean every requested table and print the row counts."""
    args = parse_args()
    data_dir = Path(args.data_dir)
    raw_dir = data_dir / "imdb" / "raw"
    clean_dir = data_dir / "imdb" / "clean"

    spark = (
        SparkSession.builder
        .appName("clean_imdb")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")


    clean = {}
    for name in ("title_basics", "name_basics"):
        if name not in args.tables:
            clean[name] = spark.read.parquet(str(clean_dir / name))

    summary = []
    for name in TABLE_ORDER:
        if name not in args.tables:
            continue
        log.info("Cleaning %s", name)
        raw = spark.read.parquet(str(raw_dir / name))
        output = clean_dir / name
        CLEANERS[name](raw, clean).write.mode("overwrite").parquet(
            str(output))
        # Read the written table back, so later tables are checked
        # against the stored result instead of recomputing it.
        clean[name] = spark.read.parquet(str(output))
        rows_in = raw.count()
        rows_out = clean[name].count()
        log.info("%s: %d -> %d rows", name, rows_in, rows_out)
        summary.append((name, rows_in, rows_out, rows_in - rows_out))

    print("\n=== Clean layer summary ===")
    spark.createDataFrame(
        summary,
        "table string, raw_rows long, clean_rows long, dropped_rows long",
    ).show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()