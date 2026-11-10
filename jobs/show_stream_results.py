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


def kafka_rows(events):
    """Rows read from Kafka between the first and the last event.

    Computed from the offsets, so it does not depend on every event having
    been recorded. Returns None for queries that do not read Kafka.
    """
    def total(offsets):
        # Kafka offsets look like {"topic": {"0": 120, "1": 98}}, either as
        # a dict or as a JSON string depending on the Spark version.
        if isinstance(offsets, str):
            try:
                offsets = json.loads(offsets)
            except json.JSONDecodeError:
                return None
        if not isinstance(offsets, dict):
            return None
        try:
            return sum(sum(int(v) for v in parts.values())
                       for parts in offsets.values())
        except (AttributeError, TypeError, ValueError):
            return None

    sources = [e["sources"][0] for e in events if e.get("sources")]
    if not sources:
        return None
    first = total(sources[0].get("endOffset"))
    last = total(sources[-1].get("endOffset"))
    if first is None or last is None:
        return None
    return last - first + events[0].get("numInputRows", 0)


def summarize_progress(metrics_file):
    """Print per-query totals computed from the saved progress events."""
    by_query = {}
    for line in metrics_file.read_text(encoding="utf-8").splitlines():
        progress = json.loads(line)
        by_query.setdefault(progress["name"], []).append(progress)
    for name, events in sorted(by_query.items()):
        events.sort(key=lambda e: e["timestamp"])
        stats = {
            "batches": len({e["batchId"] for e in events
                            if e.get("numInputRows", 0) > 0}),
            "input_rows": sum(e.get("numInputRows", 0) for e in events),
            "late_dropped": sum(
                op.get("numRowsDroppedByWatermark", 0)
                for e in events for op in e.get("stateOperators", [])),
            "max_input_rate": max(
                e.get("inputRowsPerSecond", 0.0) for e in events),
            "max_processed_rate": max(
                e.get("processedRowsPerSecond", 0.0) for e in events),
            "first_event": events[0]["timestamp"],
            "last_event": events[-1]["timestamp"],
        }
        rows = kafka_rows(events)
        if rows is not None:
            stats["kafka_rows_from_offsets"] = rows
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
    parser.add_argument("--output-dir",
                        help="Output folder (default: DATA_DIR/streaming)")
    args = parser.parse_args()
    out_dir = (Path(args.output_dir) if args.output_dir
               else Path(args.data_dir) / "streaming")

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

        tests = matched.filter(F.col("user") == "late-data-test")
        if tests.count() > 0:
            print("Synthetic late-data test events:")
            tests.select("event_time", "kafka_time", "wiki", "page_title",
                         "imdb_name").orderBy("event_time") \
                .show(truncate=False)

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
