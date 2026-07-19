"""
Kafka producer for EmbraceAI conversation turns.

Reads newline-delimited JSON conversation turns from a source file and
publishes each one to the `embraceai-conversation-turns` Kafka topic,
keyed by session_id so all turns from a given session land on the same
partition (and therefore stay in order).

This is the ingestion side of the real-time Kafka + S3 pipeline described
in docs/PRD-realtime-kafka-s3-pipeline.md. It is independent of, and does
not replace, the synchronous FastAPI inference endpoint used for live
conversations.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, Optional

from kafka import KafkaProducer
from kafka.errors import KafkaError

logger = logging.getLogger(__name__)

TOPIC = "embraceai-conversation-turns"
REQUIRED_FIELDS = ("session_id", "user_text", "category")


class ConversationTurnValidationError(ValueError):
    """Raised when a source record is missing a required field."""


def _validate_turn(record: dict) -> None:
    missing = [f for f in REQUIRED_FIELDS if f not in record]
    if missing:
        raise ConversationTurnValidationError(
            f"conversation turn missing required field(s): {', '.join(missing)}"
        )


def _iter_turns(source_path: Path) -> Iterator[dict]:
    """Yield conversation turn dicts from a newline-delimited JSON file."""
    with source_path.open("r", encoding="utf-8") as f:
        for line_number, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError as e:
                raise ValueError(f"{source_path}:{line_number}: invalid JSON ({e})") from e
            _validate_turn(record)
            yield record


def build_producer(
    bootstrap_servers: str,
    acks: str = "all",
    retries: int = 5,
    retry_backoff_ms: int = 500,
) -> KafkaProducer:
    """Construct a KafkaProducer configured for durability over latency.

    acks="all" waits for every in-sync replica to confirm the write before
    the send is considered successful (PRD F2). Retries are enabled so a
    transient broker hiccup doesn't silently drop a conversation turn.
    """
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers.split(","),
        acks=acks,
        retries=retries,
        retry_backoff_ms=retry_backoff_ms,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def publish_turn(producer: KafkaProducer, turn: dict, topic: str = TOPIC):
    """Publish a single conversation turn, keyed by session_id (PRD F1)."""
    enriched = dict(turn)
    enriched.setdefault("ingested_at", datetime.now(timezone.utc).isoformat())
    return producer.send(topic, key=enriched["session_id"], value=enriched)


def publish_file(
    source_path: Path,
    bootstrap_servers: str,
    topic: str = TOPIC,
    dry_run: bool = False,
) -> int:
    """Publish every conversation turn in `source_path` to Kafka.

    Returns the number of turns successfully published. Raises on the
    first turn that fails after retries are exhausted rather than
    silently skipping it -- a mental-health conversation record is
    exactly the kind of data this pipeline shouldn't quietly lose.
    """
    count = 0
    producer: Optional[KafkaProducer] = None
    try:
        if not dry_run:
            producer = build_producer(bootstrap_servers)

        for turn in _iter_turns(source_path):
            if dry_run:
                logger.info("[dry-run] would publish: %s", turn)
                count += 1
                continue

            future = publish_turn(producer, turn, topic=topic)
            try:
                metadata = future.get(timeout=10)
            except KafkaError:
                logger.exception(
                    "failed to publish turn for session_id=%s after retries",
                    turn.get("session_id"),
                )
                raise
            logger.info(
                "published session_id=%s -> topic=%s partition=%s offset=%s",
                turn.get("session_id"), metadata.topic, metadata.partition, metadata.offset,
            )
            count += 1
    finally:
        if producer is not None:
            producer.flush()
            producer.close()
    return count


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Publish EmbraceAI conversation turns to Kafka."
    )
    parser.add_argument(
        "source",
        type=Path,
        help='Path to a newline-delimited JSON file of conversation turns '
             '(each line: {"session_id": ..., "user_text": ..., "category": ...})',
    )
    parser.add_argument(
        "--bootstrap-servers",
        default="localhost:9092",
        help="Comma-separated Kafka bootstrap servers (default: %(default)s)",
    )
    parser.add_argument(
        "--topic",
        default=TOPIC,
        help="Kafka topic to publish to (default: %(default)s)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse and validate the source file without connecting to Kafka",
    )
    return parser.parse_args(argv)


def main(argv=None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    args = parse_args(argv)

    if not args.source.exists():
        print(f"error: source file not found: {args.source}", file=sys.stderr)
        return 1

    try:
        count = publish_file(
            args.source,
            bootstrap_servers=args.bootstrap_servers,
            topic=args.topic,
            dry_run=args.dry_run,
        )
    except (ConversationTurnValidationError, ValueError) as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    except KafkaError as e:
        print(f"error: Kafka publish failed: {e}", file=sys.stderr)
        return 1

    print(f"published {count} conversation turn(s) to {args.topic}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
