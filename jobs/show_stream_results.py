"""Print a summary of the streaming outputs and metrics.

Usage (inside the pyspark_notebook container):
    spark-submit /home/jovyan/jobs/show_stream_results.py
"""

import argparse
import json
from pathlib import Path

from pyspark.errors import AnalysisException
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def summarize_progress(metrics_file):
    """Print per-query totals computed from the saved progress lines."""
    totals = {}
    for line in metrics_file.read_text(encoding="utf-8").splitlines():
        progress = json.loads(line)
        stats = totals.setdefault(progress["name"], {
            "batches": 0, "input_rows": 0, "late_dropped": 0,
            "max_input_rate": 0.0, "max_processed_rate": 0.0,
        })
        stats["batches"] += 1
        stats["input_rows"] += progress.get("numInputRows", 0)
        stats["late_dropped"] += sum(
            op.get("numRowsDroppedByWatermark", 0)
            for op in progress.get("stateOperators", [])
        )
        stats["max_input_rate"] = max(
            stats["max_input_rate"], progress.get("inputRowsPerSecond", 0.0))
        stats["max_processed_rate"] = max(
            stats["max_processed_rate"],
            progress.get("processedRowsPerSecond", 0.0))
    for name, stats in sorted(totals.items()):
        print(name, stats)


def read_output(spark, path):
    """Read a streaming output, or return None if nothing was written."""
    try:
        return spark.read.parquet(str(path))
    except AnalysisException:
        print(f"No data yet in {path.name}")
        return None


def main():
    """Show the main tables produced by stream_wiki_imdb.py."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--data-dir", default="/home/jovyan/data")
    out_dir = Path(parser.parse_args().data_dir) / "streaming"

    spark = SparkSession.builder.appName("show_results").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    matched = read_output(spark, out_dir / "matched_events")
    if matched is not None:
        print(f"Matched events: {matched.count()}")
        matched.groupBy("wiki", "entity_type", "match_quality").count() \
            .orderBy("wiki", "entity_type").show(50, truncate=False)
        matched.select("event_time", "wiki", "page_title", "imdb_id",
                       "imdb_name", "match_quality") \
            .orderBy(F.col("event_time").desc()).show(20, truncate=False)

    windows = read_output(spark, out_dir / "edits_per_window")
    if windows is not None:
        windows.orderBy("window_start", "wiki").show(50, truncate=False)
    trending = read_output(spark, out_dir / "trending_entities")
    if trending is not None:
        trending.orderBy(F.col("edits").desc()).show(20, truncate=False)

    metrics_file = out_dir / "progress.jsonl"
    if metrics_file.exists():
        summarize_progress(metrics_file)
    spark.stop()


if __name__ == "__main__":
    main()
