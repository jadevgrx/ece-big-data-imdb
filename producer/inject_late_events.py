"""Send synthetic Wikipedia events to Kafka to test late data handling.

The script publishes, for one page, an event dated now (on time), waits
--pause seconds so that the streaming job processes it in its own
micro-batch (Spark only moves the watermark at the end of a micro-batch),
then publishes an event dated --minutes-late minutes ago (late), in the
same format as the real producer. Both are marked as tests (user
"late-data-test", domain "test.invalid") so they can be told apart from
real edits. With a
watermark shorter than --minutes-late, the streaming job must keep both in
matched_events but count only the on-time one in the windowed aggregations,
and report the late one in numRowsDroppedByWatermark.

Usage (from the host, while the streaming job and the producer run):
    docker compose run --rm --entrypoint python producer \\
        inject_late_events.py --minutes-late 30
"""

import argparse
import datetime
import json
import time
import uuid

from confluent_kafka import Producer


def make_event(title, wiki, event_time):
    """Build a minimal recentchange event dated event_time (UTC)."""
    return {
        "meta": {
            "id": str(uuid.uuid4()),
            "dt": event_time.isoformat(timespec="seconds").replace(
                "+00:00", "Z"),
            "domain": "test.invalid",
        },
        "type": "edit",
        "namespace": 0,
        "title": title,
        "title_url": None,
        "wiki": wiki,
        "user": "late-data-test",
        "bot": False,
        "minor": False,
        "timestamp": int(event_time.timestamp()),
        "length": {"old": 100, "new": 100},
    }


def send(producer, args, label, event_time):
    """Publish one synthetic event and wait until Kafka has it."""
    event = make_event(args.title, args.wiki, event_time)
    producer.produce(args.topic, key=args.title.encode("utf-8"),
                     value=json.dumps(event).encode("utf-8"))
    producer.flush(30)
    sent_at = datetime.datetime.now(datetime.timezone.utc)
    print(f"Sent {label} event at {sent_at:%H:%M:%S} UTC: {args.title} "
          f"({args.wiki}) dated {event['meta']['dt']}")


def main():
    """Publish one on-time and one late event for the same page."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--bootstrap-servers", default="kafka:19092")
    parser.add_argument("--topic", default="wikistreams")
    parser.add_argument("--title", default="Inception")
    parser.add_argument("--wiki", default="enwiki")
    parser.add_argument("--minutes-late", type=float, default=30)
    parser.add_argument("--pause", type=float, default=75,
                        help="Seconds between the two events, longer than "
                             "two triggers of the streaming job")
    args = parser.parse_args()

    producer = Producer({"bootstrap.servers": args.bootstrap_servers})
    now = datetime.datetime.now(datetime.timezone.utc)
    send(producer, args, "on time", now)
    print(f"Waiting {args.pause:.0f} s before sending the late event")
    time.sleep(args.pause)
    late = now - datetime.timedelta(minutes=args.minutes_late)
    send(producer, args, "late", late)


if __name__ == "__main__":
    main()
