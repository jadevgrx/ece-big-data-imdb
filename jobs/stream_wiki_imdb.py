"""Match live Wikipedia edits read from Kafka against IMDb.

The job reads the raw Wikimedia events published by the producer, parses
them, normalizes the page title and joins it with the static IMDb lookup
built by wiki_imdb_lookup.py. It runs three streaming queries:

- matched_events: every matched edit, written as Parquet (no aggregation,
  so late events are never dropped here). This is the only query that does
  the join with IMDb;
- edits_per_window: 5 minute tumbling windows per wiki and entity type;
- trending_entities: 10 minute windows sliding every 5 minutes per IMDb id.

The two aggregations read the Parquet output of matched_events as a
streaming source, so the lookup is joined once per micro-batch instead of
once per query. The join is a regular shuffle join: broadcasting the lookup
(about 600,000 rows) at every micro-batch ran the driver out of memory.

Both aggregations use a watermark on the event time (meta.dt) to handle
late data: an event arriving more than --watermark after the latest event
time seen is dropped from the aggregations, and Spark reports how many rows
were dropped. Query progress is saved every --report-every seconds as JSON
lines, which gives the streaming metrics for the report.

Usage (inside the pyspark_notebook container):
    spark-submit --driver-memory 4g \\
        --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 \\
        /home/jovyan/jobs/stream_wiki_imdb.py --minutes 30
"""

import argparse
import json
import time
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (BooleanType, IntegerType, LongType,
                               StringType, StructField, StructType)

from wiki_imdb_lookup import normalize

LENGTH = StructType([
    StructField("old", IntegerType()),
    StructField("new", IntegerType()),
])
EVENT_SCHEMA = StructType([
    StructField("meta", StructType([
        StructField("id", StringType()),
        StructField("dt", StringType()),
        StructField("domain", StringType()),
    ])),
    StructField("id", LongType()),
    StructField("type", StringType()),
    StructField("namespace", IntegerType()),
    StructField("title", StringType()),
    StructField("title_url", StringType()),
    StructField("wiki", StringType()),
    StructField("user", StringType()),
    StructField("bot", BooleanType()),
    StructField("minor", BooleanType()),
    StructField("timestamp", LongType()),
    StructField("length", LENGTH),
])
# Words that show a Wikipedia page is explicitly about a film, a series
# or a person of the film industry, in the five streamed languages.
CINEMA_HINTS = (
    r"\b(?:film|movie|series|sitcom|miniseries|actor|actress|director|"
    r"série|serie|téléfilm|acteur|actrice|réalisateur|réalisatrice|"
    r"fernsehserie|schauspieler|schauspielerin|regisseur|regisseurin|"
    r"película|telenovela|actriz|directora|"
    r"attore|attrice|regista)\b"
)
YEAR_PATTERN = r"\((?:[^)]*\D)?((?:18|19|20)\d{2})(?:\D[^)]*)?\)\s*$"


def read_events(spark, bootstrap_servers, topic, starting_offsets):
    """Read the raw Kafka topic as a streaming DataFrame."""
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )


def parse_events(raw):
    """Turn Kafka records into typed Wikipedia edit events."""
    event = F.from_json(F.col("value").cast("string"), EVENT_SCHEMA)
    parsed = raw.select(event.alias("e"),
                        F.col("timestamp").alias("kafka_time"))
    suffix = F.regexp_extract(F.col("e.title"), r"\(([^)]*)\)\s*$", 1)
    year = F.regexp_extract(F.col("e.title"), YEAR_PATTERN, 1)
    return parsed.select(
        F.col("e.meta.id").alias("event_id"),
        F.coalesce(
            F.to_timestamp(F.col("e.meta.dt")),
            F.timestamp_seconds(F.col("e.timestamp")),
        ).alias("event_time"),
        "kafka_time",
        F.col("e.wiki").alias("wiki"),
        F.col("e.type").alias("change_type"),
        F.col("e.title").alias("page_title"),
        F.col("e.title_url").alias("page_url"),
        F.col("e.user").alias("user"),
        F.col("e.bot").alias("bot"),
        (F.col("e.length.new") - F.col("e.length.old")).alias("size_delta"),
        normalize(F.col("e.title")).alias("match_key"),
        (suffix != "").alias("has_suffix"),
        F.lower(suffix).rlike(CINEMA_HINTS).alias("has_cinema_hint"),
        F.when(year != "", year.cast("int")).alias("title_year"),
    )


def combine_lookups(by_key, by_key_year):
    """Merge both lookups into one table keyed by join_key.

    Entries from by_key_year are keyed "title#year", the others "title".
    A single join then handles both cases, so the stream is read once.
    """
    with_year = by_key_year.withColumn(
        "join_key", F.concat_ws("#", "match_key", "start_year"))
    without_year = by_key.withColumn("join_key", F.col("match_key"))
    return with_year.unionByName(without_year).drop("match_key")


def match_imdb(events, lookup):
    """Join events with IMDb, using the year from the title when present.

    A page whose title ends with a disambiguation that is not about cinema,
    such as "Camille (chanteuse)" or "Lightning (connector)", is a
    different subject that shares a name with an IMDb entry, so it is not
    matched. "Dune (1984 film)" must match the 1984 movie and not the most
    voted "Dune", so a year is only used together with a cinema hint, and
    those events are joined on "key#year".
    """
    candidates = events.filter(
        ~F.col("has_suffix") | F.col("has_cinema_hint"))
    use_year = F.col("has_cinema_hint") & F.col("title_year").isNotNull()
    join_key = F.when(
        use_year, F.concat_ws("#", "match_key", "title_year"),
    ).otherwise(F.col("match_key"))
    matched = candidates.withColumn("join_key", join_key).join(
        lookup, "join_key")
    # "disambiguated" matches are the most reliable: the Wikipedia title
    # itself says it is a film, a series or a cinema professional.
    # "exact_title" matches only share the name and can be homonyms.
    return matched.drop("join_key", "has_suffix").withColumn(
        "match_quality",
        F.when(F.col("has_cinema_hint"), "disambiguated")
        .otherwise("exact_title"),
    )


def edits_per_window(matched, window_length):
    """Tumbling windows: activity per wiki and entity type."""
    return (
        matched
        .groupBy(F.window("event_time", window_length),
                 "wiki", "entity_type")
        .agg(
            F.count("*").alias("edits"),
            F.approx_count_distinct("imdb_id").alias("distinct_entities"),
            F.sum(F.abs("size_delta")).alias("bytes_changed"),
            F.sum(F.col("bot").cast("int")).alias("bot_edits"),
        )
        .select(F.col("window.start").alias("window_start"),
                F.col("window.end").alias("window_end"),
                "wiki", "entity_type", "edits", "distinct_entities",
                "bytes_changed", "bot_edits")
    )


def trending_entities(matched, window_length, slide):
    """Sliding windows: number of edits per IMDb entity."""
    return (
        matched
        .groupBy(F.window("event_time", window_length, slide),
                 "imdb_id", "imdb_name", "entity_type")
        .agg(F.count("*").alias("edits"),
             F.collect_set("wiki").alias("wikis"))
        .select(F.col("window.start").alias("window_start"),
                F.col("window.end").alias("window_end"),
                "imdb_id", "imdb_name", "entity_type", "edits", "wikis")
    )


def write_parquet(df, name, out_dir, mode, trigger):
    """Start a streaming query writing Parquet with its own checkpoint."""
    return (
        df.writeStream
        .queryName(name)
        .format("parquet")
        .option("path", str(out_dir / name))
        .option("checkpointLocation", str(out_dir / "checkpoints" / name))
        .outputMode(mode)
        .trigger(processingTime=trigger)
        .start()
    )


def read_matched(spark, schema, path, watermark):
    """Read the matched_events Parquet output back as a stream."""
    return (
        spark.readStream
        .schema(schema)
        .parquet(str(path))
        .withWatermark("event_time", watermark)
    )


def wait_for_first_commit(path, timeout=300):
    """Wait until a Parquet sink has committed its first batch.

    The aggregations must only start reading matched_events once its
    _spark_metadata log exists, so they only see fully written files.
    """
    deadline = time.time() + timeout
    while not (path / "_spark_metadata" / "0").exists():
        if time.time() > deadline:
            raise TimeoutError(f"No batch committed in {path}")
        time.sleep(2)


def save_progress(spark, metrics_file, last_saved):
    """Append every new progress event of the active queries to a file.

    recentProgress keeps the last events of each query, so none is missed
    between two reports. Spark also emits "idle" events while no data
    arrives, which reuse the id of the next batch, so events are told apart
    by their timestamp, not by their batch id. last_saved maps a query name
    to the timestamp of the last event written.
    """
    with open(metrics_file, "a", encoding="utf-8") as out:
        for query in spark.streams.active:
            for item in query.recentProgress:
                # Spark 4 returns progress objects, older versions dicts.
                progress = (json.loads(item.json) if hasattr(item, "json")
                            else item)
                stamp = progress["timestamp"]
                if stamp <= last_saved.get(query.name, ""):
                    continue
                out.write(json.dumps(progress) + "\n")
                last_saved[query.name] = stamp
                rows = progress.get("numInputRows", 0)
                dropped = sum(
                    op.get("numRowsDroppedByWatermark", 0)
                    for op in progress.get("stateOperators", [])
                )
                if rows or dropped:
                    print(f"[{query.name}] batch={progress['batchId']} "
                          f"input={rows} processed/s="
                          f"{progress.get('processedRowsPerSecond', 0.0):.1f}"
                          f" late_dropped={dropped}")


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--data-dir", default="/home/jovyan/data")
    parser.add_argument("--output-dir",
                        help="Output folder (default: DATA_DIR/streaming)")
    parser.add_argument("--bootstrap-servers", default="kafka:19092")
    parser.add_argument("--topic", default="wikistreams")
    parser.add_argument("--starting-offsets", default="latest",
                        choices=["earliest", "latest"])
    parser.add_argument("--watermark", default="10 minutes")
    parser.add_argument("--trigger", default="30 seconds")
    parser.add_argument("--minutes", type=float, default=30,
                        help="Run time, 0 means until stopped")
    parser.add_argument("--report-every", type=int, default=60,
                        help="Seconds between two progress reports")
    return parser.parse_args()


def main():
    """Start the streaming queries and record their progress."""
    args = parse_args()
    data_dir = Path(args.data_dir)
    lookup_dir = data_dir / "imdb" / "wiki_lookup"
    out_dir = (Path(args.output_dir) if args.output_dir
               else data_dir / "streaming")
    out_dir.mkdir(parents=True, exist_ok=True)

    spark = (
        SparkSession.builder
        .appName("stream_wiki_imdb")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        # Never broadcast the lookup: building the broadcast at every
        # micro-batch exhausted the driver memory.
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # The static side of a stream-static join is evaluated again at every
    # micro-batch. Caching the lookups in memory avoids re-reading them
    # from disk each time.
    # The static side of a stream-static join is evaluated again at every
    # micro-batch. Caching the lookup in memory avoids re-reading it from
    # disk each time.
    lookup = combine_lookups(
        spark.read.parquet(str(lookup_dir / "by_key")),
        spark.read.parquet(str(lookup_dir / "by_key_year")),
    ).cache()
    print(f"Lookup loaded: {lookup.count()} keys")

    raw = read_events(spark, args.bootstrap_servers, args.topic,
                      args.starting_offsets)
    matched = (
        match_imdb(parse_events(raw), lookup)
        .withWatermark("event_time", args.watermark)
    )
    write_parquet(matched, "matched_events", out_dir, "append",
                  args.trigger)

    matched_path = out_dir / "matched_events"
    wait_for_first_commit(matched_path)
    matched_files = read_matched(spark, matched.schema, matched_path,
                                 args.watermark)
    write_parquet(edits_per_window(matched_files, "5 minutes"),
                  "edits_per_window", out_dir, "append", args.trigger)
    write_parquet(trending_entities(matched_files, "10 minutes",
                                    "5 minutes"),
                  "trending_entities", out_dir, "append", args.trigger)

    metrics_file = out_dir / "progress.jsonl"
    stop_at = time.time() + args.minutes * 60 if args.minutes > 0 else None
    last_saved = {}
    try:
        while stop_at is None or time.time() < stop_at:
            spark.streams.awaitAnyTermination(args.report_every)
            save_progress(spark, metrics_file, last_saved)
    except KeyboardInterrupt:
        print("Interrupted, stopping the queries")
    finally:
        for query in spark.streams.active:
            query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
