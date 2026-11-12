"""Detect data quality issues in the raw IMDb tables.

The script only diagnoses: for every check it counts the offending rows,
shows a few examples and saves a summary. Handling the issues is done in
a separate cleaning job, once the choices are made.

Types are converted with try_cast, so the IMDb null marker "\\N" and any
malformed value become null instead of failing (Spark 4 runs in ANSI mode).

Usage (inside the pyspark_notebook container):
    spark-submit --driver-memory 4g /home/jovyan/jobs/preparation/quality_checks.py
    spark-submit /home/jovyan/jobs/preparation/quality_checks.py --only death_before_birth
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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("quality_checks")


def load_tables(spark, raw_dir):
    """Read the raw Parquet tables and cast the columns used by the checks."""
    def read(name):
        return spark.read.parquet(str(raw_dir / name))

    names = read("name_basics").select(
        "nconst",
        "primaryName",
        F.col("birthYear").try_cast("int").alias("birthYear"),
        F.col("deathYear").try_cast("int").alias("deathYear"),
        "knownForTitles",
    )
    titles = read("title_basics").select(
        "tconst",
        "titleType",
        "primaryTitle",
        "isAdult",
        F.col("startYear").try_cast("int").alias("startYear"),
        F.col("endYear").try_cast("int").alias("endYear"),
        F.col("runtimeMinutes").try_cast("int").alias("runtimeMinutes"),
    )
    ratings = read("title_ratings").select(
        "tconst",
        F.col("averageRating").try_cast("double").alias("averageRating"),
        F.col("numVotes").try_cast("int").alias("numVotes"),
    )
    principals = read("title_principals").select(
        "tconst",
        F.col("ordering").try_cast("int").alias("ordering"),
        "nconst",
        "category",
    )
    crew = read("title_crew").select("tconst", "directors", "writers")
    episodes = read("title_episode").select("tconst", "parentTconst")
    akas = read("title_akas").select(
        "titleId",
        F.col("ordering").try_cast("int").alias("ordering"),
    )
    return {
        "name_basics": names,
        "title_basics": titles,
        "title_ratings": ratings,
        "title_principals": principals,
        "title_crew": crew,
        "title_episode": episodes,
        "title_akas": akas,
    }


def explode_ids(df, key, column, alias):
    """Turn a comma separated id list into one row per id, nulls removed."""
    return (
        df.select(key, F.explode(F.split(column, ",")).alias(alias))
        .filter(F.col(alias) != IMDB_NULL_MARKER)
    )


def duplicates(df, keys):
    """Return the key combinations that appear more than once."""
    return df.groupBy(*keys).count().filter(F.col("count") > 1)


def build_checks(t):
    """Return a list of (check name, table, DataFrame of offending rows)."""
    names = t["name_basics"]
    titles = t["title_basics"]
    principals = t["title_principals"]
    crew = t["title_crew"]
    episodes = t["title_episode"]
    title_ids = titles.select("tconst")
    name_ids = names.select("nconst")

    credits = (
        principals
        .filter(~F.col("category").startswith("archive"))
        .join(titles.select("tconst", "startYear"), "tconst")
        .join(names.select("nconst", "birthYear"), "nconst")
    )
    parents = titles.select(
        F.col("tconst").alias("parentTconst"),
        F.col("titleType").alias("parentType"),
    )

    return [
        # name_basics
        ("death_before_birth", "name_basics",
         names.filter(F.col("deathYear") < F.col("birthYear"))),
        ("born_in_future", "name_basics",
         names.filter(F.col("birthYear") > CURRENT_YEAR)),
        ("died_in_future", "name_basics",
         names.filter(F.col("deathYear") > CURRENT_YEAR)),
        ("implausible_lifespan", "name_basics",
         names.filter(F.col("deathYear") - F.col("birthYear")
                      > MAX_LIFESPAN)),
        ("duplicate_nconst", "name_basics", duplicates(names, ["nconst"])),
        ("orphan_known_for_title", "name_basics",
         explode_ids(names, "nconst", "knownForTitles", "tconst")
         .join(title_ids, "tconst", "left_anti")),
        # title_basics
        ("duplicate_tconst", "title_basics", duplicates(titles, ["tconst"])),
        ("end_before_start", "title_basics",
         titles.filter(F.col("endYear") < F.col("startYear"))),
        ("start_in_future", "title_basics",
         titles.filter(F.col("startYear") > CURRENT_YEAR)),
        ("non_positive_runtime", "title_basics",
         titles.filter(F.col("runtimeMinutes") <= 0)),
        ("extreme_runtime", "title_basics",
         titles.filter(F.col("runtimeMinutes") > MAX_RUNTIME)),
        ("invalid_is_adult", "title_basics",
         titles.filter(~F.col("isAdult").isin("0", "1"))),
        # title_ratings
        ("rating_out_of_range", "title_ratings",
         t["title_ratings"].filter(
             (F.col("averageRating") < 1) | (F.col("averageRating") > 10))),
        ("non_positive_votes", "title_ratings",
         t["title_ratings"].filter(F.col("numVotes") <= 0)),
        ("orphan_rating_title", "title_ratings",
         t["title_ratings"].join(title_ids, "tconst", "left_anti")),
        # title_principals
        ("orphan_principal_title", "title_principals",
         principals.join(title_ids, "tconst", "left_anti")),
        ("orphan_principal_name", "title_principals",
         principals.join(name_ids, "nconst", "left_anti")),
        ("duplicate_principal_ordering", "title_principals",
         duplicates(principals, ["tconst", "ordering"])),
        ("credit_before_birth", "title_principals",
         credits.filter(F.col("startYear") < F.col("birthYear"))),
        # title_crew
        ("orphan_crew_title", "title_crew",
         crew.join(title_ids, "tconst", "left_anti")),
        ("orphan_director", "title_crew",
         explode_ids(crew, "tconst", "directors", "nconst")
         .join(name_ids, "nconst", "left_anti")),
        ("orphan_writer", "title_crew",
         explode_ids(crew, "tconst", "writers", "nconst")
         .join(name_ids, "nconst", "left_anti")),
        # title_episode
        ("orphan_episode_title", "title_episode",
         episodes.join(title_ids, "tconst", "left_anti")),
        ("orphan_episode_parent", "title_episode",
         episodes.join(parents, "parentTconst", "left_anti")),
        ("episode_parent_not_series", "title_episode",
         episodes.join(parents, "parentTconst")
         .filter(~F.col("parentType").isin("tvSeries", "tvMiniSeries"))),
        # title_akas
        ("orphan_aka_title", "title_akas",
         t["title_akas"].join(
             title_ids.withColumnRenamed("tconst", "titleId"),
             "titleId", "left_anti")),
        ("duplicate_aka_ordering", "title_akas",
         duplicates(t["title_akas"], ["titleId", "ordering"])),
    ]


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--data-dir",
        default="/home/jovyan/data",
        help="Root data directory (default: %(default)s)",
    )
    parser.add_argument(
        "--only",
        nargs="+",
        default=None,
        help="Run only the checks with these names",
    )
    parser.add_argument(
        "--examples",
        type=int,
        default=5,
        help="Number of example rows shown per check (default: %(default)s)",
    )
    return parser.parse_args()


def main():
    """Run every check, print examples and save the summary."""
    args = parse_args()
    data_dir = Path(args.data_dir)
    raw_dir = data_dir / "imdb" / "raw"
    output_dir = data_dir / "preparation" / "quality_checks"

    spark = (
        SparkSession.builder
        .appName("quality_checks")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    tables = load_tables(spark, raw_dir)
    totals = {}
    results = []
    for check, table, issues in build_checks(tables):
        if args.only and check not in args.only:
            continue
        if table not in totals:
            totals[table] = tables[table].count()
        count = issues.count()
        pct = round(100.0 * count / totals[table], 4)
        log.info("%s (%s): %d rows, %.4f %%", check, table, count, pct)
        if count:
            print(f"\n--- {check}: {count} rows ---")
            issues.show(args.examples, truncate=False)
        results.append((check, table, totals[table], count, pct))

    summary = spark.createDataFrame(
        results,
        "check string, table string, total_rows long, "
        "issue_rows long, issue_pct double",
    ).orderBy(F.desc("issue_rows"))
    print("\n=== Data quality summary ===")
    summary.show(100, truncate=False)
    summary.coalesce(1).write.mode("overwrite").option("header", True).csv(
        str(output_dir)
    )
    log.info("Summary written to %s", output_dir)
    spark.stop()


if __name__ == "__main__":
    main()