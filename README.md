# EmbraceAI

AI-powered mental-health support system. 1st Place / Top NLP Project, AI CAMP NLP Track,

At its core, EmbraceAI uses a fine-tuned DistilBERT model to classify user intent across 12 conversational categories at 91% accuracy, served via a FastAPI inference API for live, single-conversation use.

Original core project here: https://replit.com/@YL221/EmbraceBackEndVer-2#VENV.md

## Real-time Kafka + S3 pipeline

`pipeline/` adds a parallel, streaming-oriented path alongside the synchronous FastAPI endpoint:

- **`kafka_producer.py`** — publishes conversation turns to the `embraceai-conversation-turns` Kafka topic, keyed by `session_id` so a session's turns stay ordered on one partition, with `acks=all` and retries so a turn isn't silently dropped.
- **`spark_consumer.py`** — a Spark Structured Streaming job that classifies turns in batches (via a pandas UDF, so the DistilBERT model loads once per executor rather than once per row) and writes the classified output to S3 as Parquet, partitioned by predicted label, with checkpointing for fault-tolerant resume.
- **`requirements.txt`** — `kafka-python`, `pyspark`, `transformers`, `torch`, `boto3`.

This does **not** replace the FastAPI endpoint; it's for durability and scale (a queryable, warehoused record of conversation data), not lower-latency single-conversation inference.

Full design and current status: [`docs/PRD-realtime-kafka-s3-pipeline.md`](docs/PRD-realtime-kafka-s3-pipeline.md).

**Status:** reference implementation, not yet run end-to-end against a live Kafka/Spark deployment. See the PRD's "Current Status" and "Open Questions / Risks" sections before running this against real data — in particular, the model checkpoint in `spark_consumer.py` is a placeholder, and PII/retention handling for S3 hasn't been addressed yet.

### Usage

```bash
pip install -r pipeline/requirements.txt

# Producer: publish conversation turns from a newline-delimited JSON file
python pipeline/kafka_producer.py turns.jsonl --bootstrap-servers localhost:9092

# Consumer: classify the stream and write partitioned Parquet to S3
python pipeline/spark_consumer.py \
  --bootstrap-servers localhost:9092 \
  --s3-output-path s3a://embraceai-data/conversation-turns/ \
  --checkpoint-location s3a://embraceai-data/checkpoints/conversation-turns/
```
