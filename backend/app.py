"""
FastAPI inference service for EmbraceAI.

Classifies a conversation turn's text by calling a Hugging Face-hosted
DistilBERT model through the Hugging Face Inference API (no local
transformers/torch install required on this service), and optionally
publishes the classified turn onto the same Kafka topic used by
pipeline/kafka_producer.py so it also lands in the streaming/S3 path
described in docs/PRD-realtime-kafka-s3-pipeline.md.

This is a reference implementation: it has not been run against the
real fine-tuned EmbraceAI checkpoint (HF_MODEL_ID defaults to a public
DistilBERT sentiment model as a stand-in -- see .env.example), and the
Kafka publish path has not been exercised against a live broker. Both
are wired up correctly and fail loudly/gracefully rather than silently,
but should be validated against the real checkpoint and a real broker
before this replaces the existing Replit FastAPI service.
"""

from __future__ import annotations

import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import requests
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

# Reuse the existing Kafka producer helpers from ../pipeline instead of
# duplicating the topic name / serialization logic.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from pipeline.kafka_producer import TOPIC as KAFKA_TOPIC, build_producer  # noqa: E402

load_dotenv()

logger = logging.getLogger("embraceai.backend")
logging.basicConfig(level=logging.INFO)

HF_API_TOKEN = os.environ.get("HF_API_TOKEN", "")
HF_MODEL_ID = os.environ.get("HF_MODEL_ID", "distilbert-base-uncased-finetuned-sst-2-english")
HF_INFERENCE_URL = f"https://api-inference.huggingface.co/models/{HF_MODEL_ID}"

KAFKA_ENABLED = os.environ.get("KAFKA_ENABLED", "false").strip().lower() == "true"
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

CORS_ORIGINS = [
    origin.strip()
    for origin in os.environ.get("CORS_ORIGINS", "http://localhost:5173").split(",")
    if origin.strip()
]

app = FastAPI(
    title="EmbraceAI Classification API",
    description="DistilBERT text classification via the Hugging Face Inference API, "
    "with an optional publish step onto the EmbraceAI Kafka pipeline.",
    version="0.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=False,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

# The Kafka producer is expensive to construct, so build it once at startup
# (only if KAFKA_ENABLED) and reuse it across requests, closing it cleanly
# on shutdown.
_producer = None


@app.on_event("startup")
def _startup() -> None:
    global _producer
    if KAFKA_ENABLED:
        try:
            _producer = build_producer(KAFKA_BOOTSTRAP_SERVERS)
            logger.info("Kafka producer connected to %s", KAFKA_BOOTSTRAP_SERVERS)
        except Exception:
            # Don't crash the API if Kafka is unreachable -- classification
            # should keep working even if the streaming path is down.
            logger.exception(
                "failed to construct Kafka producer for %s; "
                "classification will still work, publishing will be skipped",
                KAFKA_BOOTSTRAP_SERVERS,
            )
            _producer = None


@app.on_event("shutdown")
def _shutdown() -> None:
    if _producer is not None:
        _producer.flush()
        _producer.close()


class ClassifyRequest(BaseModel):
    session_id: str = Field(..., min_length=1, description="Stable id for the conversation session")
    user_text: str = Field(..., min_length=1, max_length=2000, description="The user's message to classify")


class ClassificationResult(BaseModel):
    label: str
    score: float


class ClassifyResponse(BaseModel):
    session_id: str
    user_text: str
    predictions: list[ClassificationResult]
    top_label: str
    model: str
    published_to_kafka: bool


def _call_hf_inference(text: str, max_retries: int = 3) -> list[dict]:
    """Call the Hugging Face Inference API for text classification.

    Hosted models on the free/serverless tier can return a 503 with an
    `estimated_time` while the model is warming up (cold start) -- retry
    a few times with a short backoff before giving up, rather than
    surfacing a transient cold-start as a hard failure.
    """
    if not HF_API_TOKEN:
        raise HTTPException(
            status_code=503,
            detail="HF_API_TOKEN is not configured; set it in backend/.env (see .env.example)",
        )

    headers = {"Authorization": f"Bearer {HF_API_TOKEN}"}
    last_error: Optional[str] = None

    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.post(
                HF_INFERENCE_URL,
                headers=headers,
                json={"inputs": text},
                timeout=30,
            )
        except requests.RequestException as e:
            last_error = str(e)
            time.sleep(min(2 ** attempt, 8))
            continue

        if resp.status_code == 200:
            data = resp.json()
            # The Inference API returns either [[{label, score}, ...]] or
            # [{label, score}, ...] depending on the model/task; normalize.
            if isinstance(data, list) and data and isinstance(data[0], list):
                return data[0]
            if isinstance(data, list):
                return data
            raise HTTPException(status_code=502, detail=f"unexpected HF response shape: {data!r}")

        if resp.status_code == 503:
            # Model is loading (cold start) -- wait and retry.
            wait_s = min(resp.json().get("estimated_time", 2), 15) if resp.text else 2
            time.sleep(wait_s)
            last_error = f"model warming up (attempt {attempt}/{max_retries})"
            continue

        raise HTTPException(
            status_code=502,
            detail=f"Hugging Face Inference API error {resp.status_code}: {resp.text[:300]}",
        )

    raise HTTPException(status_code=504, detail=f"Hugging Face Inference API unavailable: {last_error}")


@app.get("/api/health")
def health() -> dict:
    return {
        "status": "ok",
        "model": HF_MODEL_ID,
        "kafka_enabled": KAFKA_ENABLED,
        "kafka_connected": _producer is not None,
    }


@app.post("/api/classify", response_model=ClassifyResponse)
def classify(req: ClassifyRequest) -> ClassifyResponse:
    predictions_raw = _call_hf_inference(req.user_text)
    predictions = sorted(
        (ClassificationResult(label=p["label"], score=float(p["score"])) for p in predictions_raw),
        key=lambda p: p.score,
        reverse=True,
    )
    if not predictions:
        raise HTTPException(status_code=502, detail="Hugging Face Inference API returned no predictions")

    top_label = predictions[0].label

    published = False
    if KAFKA_ENABLED and _producer is not None:
        turn = {
            "session_id": req.session_id,
            "user_text": req.user_text,
            "category": top_label,
            "ingested_at": datetime.now(timezone.utc).isoformat(),
        }
        try:
            future = _producer.send(KAFKA_TOPIC, key=req.session_id, value=turn)
            future.get(timeout=10)
            published = True
        except Exception:
            # Classification already succeeded; a Kafka publish failure
            # shouldn't turn into a 500 for the caller, just log it.
            logger.exception("failed to publish classified turn to Kafka for session_id=%s", req.session_id)

    return ClassifyResponse(
        session_id=req.session_id,
        user_text=req.user_text,
        predictions=predictions,
        top_label=top_label,
        model=HF_MODEL_ID,
        published_to_kafka=published,
    )
