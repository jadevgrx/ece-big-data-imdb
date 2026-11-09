"""Stream Wikipedia recent changes from Wikimedia EventStreams into Kafka.

The producer opens a single Server-Sent Events connection to the public
Wikimedia "recentchange" stream, keeps the article edits and page creations
of a few language editions of Wikipedia, and publishes each event unchanged
(raw JSON) to a Kafka topic. Matching the events against IMDb is done later
by the Spark Structured Streaming job, so this script stays a thin, generic
ingestion layer.

Usage (from the host, through Docker Compose):
    docker compose run --rm producer --minutes 30
"""

import argparse
import json
import logging
import time

import requests
from confluent_kafka import KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
# Wikimedia asks every client to identify itself with a contact point.
USER_AGENT = (
    "ece-big-data-imdb/1.0 (ECE Paris student project; "
    "https://github.com/jadevgrx/ece-big-data-imdb)"
)
DEFAULT_WIKIS = ["enwiki", "frwiki", "dewiki", "eswiki", "itwiki"]
KEPT_TYPES = {"edit", "new"}
ARTICLE_NAMESPACE = 0
LOG_EVERY = 500

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("wiki_producer")


def ensure_topic(bootstrap_servers, topic, partitions):
    """Create the Kafka topic if it does not exist yet."""
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    if topic in admin.list_topics(timeout=30).topics:
        log.info("Topic %s already exists", topic)
        return
    new_topic = NewTopic(topic, num_partitions=partitions,
                         replication_factor=1)
    for future in admin.create_topics([new_topic]).values():
        future.result()
    log.info("Created topic %s with %d partitions", topic, partitions)


def iter_events(session, last_event_id=None):
    """Yield (event_id, data) pairs read from the SSE stream.

    Server-Sent Events are blocks of "field: value" lines separated by an
    empty line. Only the "id" and "data" fields are needed here.
    """
    headers = {"Accept": "text/event-stream"}
    if last_event_id:
        # Resume after the last event seen if the connection was cut.
        headers["Last-Event-ID"] = last_event_id
    with session.get(STREAM_URL, headers=headers, stream=True,
                     timeout=(10, 60)) as response:
        response.raise_for_status()
        event_id = None
        data_lines = []
        for line in response.iter_lines(decode_unicode=True):
            if line is None:
                continue
            if line == "":
                if data_lines:
                    yield event_id, "\n".join(data_lines)
                event_id = None
                data_lines = []
            elif line.startswith("id:"):
                event_id = line[3:].strip()
            elif line.startswith("data:"):
                data_lines.append(line[5:].lstrip())


def is_relevant(change, wikis):
    """Keep article edits and creations on the selected wikis."""
    return (
        change.get("wiki") in wikis
        and change.get("type") in KEPT_TYPES
        and change.get("namespace") == ARTICLE_NAMESPACE
    )


def publish(producer, topic, change, raw):
    """Send one event to Kafka, keyed by page title."""
    key = change.get("title", "").encode("utf-8")
    while True:
        try:
            producer.produce(topic, key=key, value=raw.encode("utf-8"))
            break
        except BufferError:
            # Local queue full: let the client deliver pending messages.
            producer.poll(1)
    producer.poll(0)


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--bootstrap-servers", default="kafka:19092")
    parser.add_argument("--topic", default="wikistreams")
    parser.add_argument("--partitions", type=int, default=3)
    parser.add_argument(
        "--wikis",
        nargs="+",
        default=DEFAULT_WIKIS,
        help="Wikipedia editions to keep (default: %(default)s)",
    )
    parser.add_argument(
        "--minutes",
        type=float,
        default=30,
        help="How long to stream, 0 means no limit (default: %(default)s)",
    )
    return parser.parse_args()


def main():
    """Read the Wikimedia stream and forward relevant events to Kafka."""
    args = parse_args()
    wikis = set(args.wikis)
    ensure_topic(args.bootstrap_servers, args.topic, args.partitions)
    producer = Producer({
        "bootstrap.servers": args.bootstrap_servers,
        "client.id": "wiki_producer",
        "compression.type": "lz4",
    })
    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT

    stop_at = time.time() + args.minutes * 60 if args.minutes > 0 else None
    received = kept = 0
    last_event_id = None
    backoff = 1
    log.info("Streaming %s into topic %s", sorted(wikis), args.topic)

    while stop_at is None or time.time() < stop_at:
        try:
            for event_id, raw in iter_events(session, last_event_id):
                last_event_id = event_id or last_event_id
                backoff = 1
                received += 1
                try:
                    change = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                if is_relevant(change, wikis):
                    publish(producer, args.topic, change, raw)
                    kept += 1
                if received % LOG_EVERY == 0:
                    log.info("received=%d kept=%d", received, kept)
                if stop_at is not None and time.time() >= stop_at:
                    break
        except (requests.RequestException, KafkaException) as error:
            # Reconnect with exponential backoff, capped at one minute,
            # so the public service is not hammered after a failure.
            log.warning("Stream error (%s), retrying in %ds", error, backoff)
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)

    producer.flush(30)
    log.info("Done: received=%d kept=%d", received, kept)


if __name__ == "__main__":
    main()
