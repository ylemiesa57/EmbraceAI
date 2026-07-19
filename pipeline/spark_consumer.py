"""
Spark Structured Streaming consumer for EmbraceAI conversation turns.

Subscribes to the `embraceai-conversation-turns` Kafka topic, classifies
each conversation turn's `user_text` using the fine-tuned DistilBERT model
(batched via a pandas UDF so the model loads once per executor process
rather than once per row), and writes the classified stream to S3 as
Parquet, partitioned by predicted label, with checkpointing for
fault-tolerant resume.

See docs/PRD-realtime-kafka-s3-pipeline.md for the full design. This is a
reference implementation (see PRD Section 7): it has not been run against
a live Kafka cluster or Spark deployment, and MODEL_NAME below is a
placeholder that needs to point at the real fine-tuned checkpoint before
this can classify real data (see PRD Section 8).
"""

from __future__ import annotations

import argparse

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, pandas_udf
from pyspark.sql.types import StringType, StructField, StructType

TOPIC = "embraceai-conversation-turns"

# Placeholder -- swap for the real fine-tuned checkpoint (local path, private
# Hugging Face repo, or S3 URI) before running against real data.
MODEL_NAME = "distilbert-base-uncased-finetuned-embraceai"

TURN_SCHEMA = StructType(
    [
        StructField("session_id", StringType(), nullable=False),
        StructField("user_text", StringType(), nullable=False),
        StructField("category", StringType(), nullable=True),
        StructField("ingested_at", StringType(), nullable=True),
    ]
)

# Lazily initialized per executor process; see _get_classifier below.
_classifier = None


def _get_classifier(model_name: str):
    """Load the fine-tuned DistilBERT classification pipeline once per
    executor process (PRD F4/N2), rather than re-loading model weights for
    every batch. Kept as a module-level cache since pandas UDFs run inside
    long-lived executor processes, so this only pays the load cost once.
    """
    global _classifier
    if _classifier is None:
        from transformers import pipeline  # heavy import: only needed on executors

        _classifier = pipeline(
            "text-classification",
            model=model_name,
            tokenizer=model_name,
            truncation=True,
        )
    return _classifier


def make_classify_udf(model_name: str = MODEL_NAME):
    """Build a pandas UDF that classifies a batch of `user_text` values in
    one model call rather than one call per row (PRD F4).
    """

    @pandas_udf(StringType())
    def classify_batch(user_text: pd.Series) -> pd.Series:
        classifier = _get_classifier(model_name)
        texts = user_text.fillna("").tolist()
        if not texts:
            return pd.Series([], dtype=str)
        results = classifier(texts)
        return pd.Series([r["label"] for r in results])

    return classify_batch


def build_pipeline(
    spark: SparkSession,
    bootstrap_servers: str,
    s3_output_path: str,
    checkpoint_location: str,
    model_name: str = MODEL_NAME,
    kafka_topic: str = TOPIC,
    starting_offsets: str = "latest",
):
    """Wire up the read -> parse -> classify -> write streaming pipeline.

    Returns the StreamingQuery (rather than blocking on awaitTermination
    itself) so callers -- including future tests, via a memory/console
    sink -- can control start/await/stop without this function owning the
    process lifecycle (PRD N3).
    """
    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", kafka_topic)
        .option("startingOffsets", starting_offsets)
        .load()
    )

    parsed = raw_stream.select(
        from_json(col("value").cast("string"), TURN_SCHEMA).alias("turn")
    ).select("turn.*")

    classify_batch = make_classify_udf(model_name)
    classified = parsed.withColumn("predicted_label", classify_batch(col("user_text")))

    query = (
        classified.writeStream.format("parquet")
        .option("path", s3_output_path)
        .option("checkpointLocation", checkpoint_location)
        .partitionBy("predicted_label")
        .outputMode("append")
        .start()
    )
    return query


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Classify EmbraceAI conversation turns from Kafka and "
        "write the results to S3 as partitioned Parquet."
    )
    parser.add_argument(
        "--bootstrap-servers",
        default="localhost:9092",
        help="Comma-separated Kafka bootstrap servers (default: %(default)s)",
    )
    parser.add_argument(
        "--topic",
        default=TOPIC,
        help="Kafka topic to subscribe to (default: %(default)s)",
    )
    parser.add_argument(
        "--s3-output-path",
        required=True,
        help="S3 path to write classified Parquet output to, "
        "e.g. s3a://embraceai-data/conversation-turns/",
    )
    parser.add_argument(
        "--checkpoint-location",
        required=True,
        help="S3 or HDFS path for Structured Streaming checkpoint state, "
        "e.g. s3a://embraceai-data/checkpoints/conversation-turns/",
    )
    parser.add_argument(
        "--model-name",
        default=MODEL_NAME,
        help="Hugging Face model identifier or path for the fine-tuned "
        "DistilBERT classifier (default: %(default)s)",
    )
    parser.add_argument(
        "--starting-offsets",
        default="latest",
        choices=["latest", "earliest"],
        help="Kafka startingOffsets to use on a fresh (no-checkpoint) run "
        "(default: %(default)s)",
    )
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)

    spark = SparkSession.builder.appName("embraceai-conversation-classifier").getOrCreate()

    query = build_pipeline(
        spark,
        bootstrap_servers=args.bootstrap_servers,
        s3_output_path=args.s3_output_path,
        checkpoint_location=args.checkpoint_location,
        model_name=args.model_name,
        kafka_topic=args.topic,
        starting_offsets=args.starting_offsets,
    )
    query.awaitTermination()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
