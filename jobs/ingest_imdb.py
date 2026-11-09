"""Download the IMDb non-commercial datasets and store them as raw Parquet.

The raw layer is a faithful copy of the source files: every column is kept
as a string and the IMDb null marker ("\\N") is left untouched, so that the
data preparation step can measure null, missing and empty values exactly as
they appear in the original TSV files. Typing and cleaning happen later, in
the preparation jobs.

Usage (inside the pyspark_notebook container):
    spark-submit --driver-memory 4g /home/jovyan/jobs/ingest_imdb.py
    spark-submit /home/jovyan/jobs/ingest_imdb.py --tables title.ratings
"""

import argparse
import logging
import shutil
import urllib.request
from pathlib import Path

from pyspark.sql import SparkSession

IMDB_BASE_URL = "https://datasets.imdbws.com"
IMDB_TABLES = [
    "name.basics",
    "title.akas",
    "title.basics",
    "title.crew",
    "title.episode",
    "title.principals",
    "title.ratings",
]
USER_AGENT = "ece-big-data-imdb/1.0 (ECE Paris student project)"
CHUNK_SIZE = 1024 * 1024

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("ingest_imdb")


def download_table(table, download_dir, force=False):
    """Download one IMDb .tsv.gz file, skipping it if already present."""
    target = download_dir / f"{table}.tsv.gz"
    if target.exists() and not force:
        log.info("%s already downloaded, skipping", target.name)
        return target

    url = f"{IMDB_BASE_URL}/{target.name}"
    partial = target.with_name(target.name + ".part")
    log.info("Downloading %s", url)
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request) as response:
        with open(partial, "wb") as out:
            shutil.copyfileobj(response, out, length=CHUNK_SIZE)
    # Rename only once the download is complete, so an interrupted run
    # never leaves a truncated file that would be skipped next time.
    partial.rename(target)
    return target


def convert_table(spark, table, source, parquet_dir):
    """Read one IMDb TSV file with Spark and write it as Parquet."""
    df = (
        spark.read
        .option("sep", "\t")
        .option("header", True)
        # IMDb files do not use quoting: titles can contain a lone '"',
        # so quote handling must be disabled or rows get merged.
        .option("quote", "")
        .option("inferSchema", False)
        .csv(str(source))
    )
    output = parquet_dir / table.replace(".", "_")
    df.write.mode("overwrite").parquet(str(output))
    return output


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
        choices=IMDB_TABLES,
        default=IMDB_TABLES,
        help="Tables to ingest (default: all)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Download the files again even if they already exist",
    )
    return parser.parse_args()


def main():
    """Download every requested table and convert it to Parquet."""
    args = parse_args()
    data_dir = Path(args.data_dir)
    download_dir = data_dir / "imdb" / "tsv"
    parquet_dir = data_dir / "imdb" / "raw"
    download_dir.mkdir(parents=True, exist_ok=True)
    parquet_dir.mkdir(parents=True, exist_ok=True)

    spark = (
        SparkSession.builder
        .appName("ingest_imdb")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    for table in args.tables:
        source = download_table(table, download_dir, force=args.force)
        output = convert_table(spark, table, source, parquet_dir)
        rows = spark.read.parquet(str(output)).count()
        log.info("%s -> %s (%d rows)", table, output, rows)

    spark.stop()


if __name__ == "__main__":
    main()
