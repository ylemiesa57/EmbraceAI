# PRD: Real-Time Kafka + S3 Data Pipeline for EmbraceAI
**Author:** Yaphet Lemiesa
**Status:** Draft — documents commits added to `ylemiesa57/EmbraceAI` under `pipeline/`
**Related files:** `pipeline/kafka_producer.py`, `pipeline/spark_consumer.py`, `pipeline/requirements.txt`, `README.md`
---
## 1. Summary
EmbraceAI is an AI-powered mental-health support system (1st Place / Top NLP Project, AI CAMP NLP Track, HackMIT) built around a fine-tuned DistilBERT model that classifies user intent across 12 conversational categories at 91% accuracy, served via a FastAPI inference API.
This PRD covers a new addition to that project: a real-time data pipeline that streams conversation turns through **Apache Kafka**, classifies them at scale with **Spark Structured Streaming** using the existing DistilBERT model, and warehouses the classified output to **AWS S3** as partitioned Parquet for downstream analysis.
## 2. Problem Statement
The original EmbraceAI system classifies conversation turns synchronously, one request at a time, through the FastAPI inference endpoint. That's a reasonable design for serving a single live conversation, but it doesn't provide:
- A durable, ordered record of conversation turns as they happen
- A way to classify and analyze conversation data at a volume beyond what a single synchronous API can handle
- A queryable, warehoused dataset of classified conversations for downstream analysis (e.g., aggregate trend review, model evaluation, retraining data collection)
This pipeline addresses that gap without replacing the existing synchronous API, which remains the path for live, single-conversation inference.
## 3. Goals
- Ingest conversation turns through a fault-tolerant, ordered streaming layer (Kafka) rather than only handling them inline in the API request path.
- Classify conversation turns at scale using the same fine-tuned DistilBERT model already validated in the core EmbraceAI system, batched via Spark rather than one-at-a-time.
- Warehouse classified turns to S3 in a queryable, partitioned format (Parquet, partitioned by predicted label) to support downstream analysis.
## 4. Non-Goals
- This pipeline does **not** replace the existing FastAPI real-time inference endpoint; it's a parallel, batch/streaming-oriented path for durability and scale, not a lower-latency alternative.
- This PRD does not cover model retraining, alerting on high-risk classifications, or any clinical/human-in-the-loop escalation workflow. Those would be reasonable follow-on work but are out of scope for these commits.
- No new UI or user-facing surface is introduced.
## 5. Requirements
### 5.1 Functional
| ID | Requirement |
|---|---|
| F1 | Producer publishes each conversation turn (session ID, user text, category, ingestion timestamp) as a single Kafka message, keyed by session ID so all turns from one session land in order on the same partition. |
| F2 | Producer acknowledges only once all in-sync replicas confirm the write (`acks=all`), and retries transient broker errors rather than silently dropping a turn. |
| F3 | Consumer reads from Kafka via Spark Structured Streaming and classifies each turn using the fine-tuned DistilBERT model. |
| F4 | Classification is batched (via a pandas UDF) so the model loads once per executor process rather than once per row. |
| F5 | Classified output is written to S3 as Parquet, partitioned by predicted label, with checkpointing so the streaming job can resume without reprocessing or data loss after a restart. |
### 5.2 Non-Functional
| ID | Requirement |
|---|---|
| N1 | The pipeline should scale horizontally on the Spark side independently of the Kafka ingestion rate. |
| N2 | Model inference should not become a per-row bottleneck; batching is required (see F4). |
| N3 | The producer and consumer should be independently runnable and testable components, not a single monolithic script. |
## 6. Technical Design
**Topic:** `embraceai-conversation-turns`
**Producer (`kafka_producer.py`):**
Reads newline-delimited JSON conversation turns from a source file, tags each with an ingestion timestamp, and publishes to the topic above, partitioned by `session_id`.
**Consumer (`spark_consumer.py`):**
A Spark Structured Streaming job that:
1. Subscribes to the Kafka topic and parses incoming JSON against a defined schema (`session_id`, `user_text`, `category`, `ingested_at`).
2. Applies a pandas UDF that lazily loads the fine-tuned DistilBERT model (via Hugging Face `transformers`) once per executor and classifies batches of `user_text`.
3. Writes the classified stream to S3 as Parquet, partitioned by `predicted_label`, with a configurable checkpoint location for fault-tolerant resume.
**Dependencies (`requirements.txt`):** `kafka-python`, `pyspark`, `transformers`, `torch`, `boto3`.
## 7. Current Status
These commits are a **reference implementation of the architecture**, not a tested, deployed service. Specifically:
- The pipeline has not been run end-to-end against a live Kafka cluster or Spark deployment.
- The model identifier in `spark_consumer.py` (`distilbert-base-uncased-finetuned-embraceai`) is a placeholder and needs to point at the actual fine-tuned checkpoint before this can run against real data.
- No integration tests exist yet for either the producer or the consumer.
## 8. Open Questions / Risks
- **Model checkpoint location:** where the real fine-tuned DistilBERT weights are hosted (local path vs. a private Hugging Face repo vs. S3) needs to be decided before this is runnable.
- **PII / sensitive data handling:** conversation turns from a mental-health support tool are sensitive by nature. This PRD does not yet address encryption at rest/in transit, access controls on the S3 bucket, or retention policy, all of which should be resolved before any real user data flows through this pipeline.
- **Cost:** running a persistent Spark Structured Streaming job plus a Kafka cluster has ongoing infrastructure cost that hasn't been estimated here.
- **Testing plan:** no test plan yet exists for validating classification accuracy holds up in the streaming/batched path versus the original synchronous API.
## 9. Appendix: File Manifest
- `pipeline/kafka_producer.py`
- `pipeline/spark_consumer.py`
- `pipeline/requirements.txt`
- `README.md` (updated to document the pipeline and the AI CAMP hackathon result)
