"""NLP service: KeyBERT topic extraction over HTTP.

Runs as its own container so the ~90 MB sentence-transformer (plus torch) is
loaded once, instead of once per Spark executor.
"""

from __future__ import annotations

import logging
import os
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from pydantic import BaseModel, Field

from extractor import TopicExtractor

MODEL_NAME = os.getenv("NLP_MODEL_NAME", "sentence-transformers/all-MiniLM-L6-v2")
MODEL_VERSION = os.getenv("NLP_MODEL_VERSION", "keybert-v1")
TOP_N = int(os.getenv("NLP_TOP_N", "5"))
MIN_SCORE = float(os.getenv("NLP_MIN_SCORE", "0.15"))
MAX_DOCUMENTS = int(os.getenv("NLP_MAX_DOCUMENTS", "1000"))

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("nlp-service")


class Document(BaseModel):
    post_id: str
    text: str = ""


class ExtractRequest(BaseModel):
    documents: list[Document] = Field(max_length=MAX_DOCUMENTS)


class TopicResult(BaseModel):
    post_id: str
    topics: list[str]
    scores: list[float]
    model_version: str


class ExtractResponse(BaseModel):
    results: list[TopicResult]


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Loaded here rather than at import time so tests and tooling that import
    # this module do not pay for the model.
    from keybert import KeyBERT
    from sentence_transformers import SentenceTransformer

    started = time.time()
    app.state.extractor = TopicExtractor(KeyBERT(SentenceTransformer(MODEL_NAME, device="cpu")),
                                         top_n=TOP_N, min_score=MIN_SCORE)
    log.info("model %s loaded in %.1fs", MODEL_NAME, time.time() - started)
    yield


app = FastAPI(title="Reddit Trend Tracker NLP service", lifespan=lifespan)


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "model_version": MODEL_VERSION}


@app.post("/extract-topics", response_model=ExtractResponse)
def extract_topics(body: ExtractRequest, request: Request) -> ExtractResponse:
    # A sync handler: FastAPI runs it in a worker thread, so CPU-bound inference
    # does not block the event loop (and /health stays responsive).
    started = time.time()
    extracted = request.app.state.extractor.extract([d.text for d in body.documents])
    results = [
        TopicResult(post_id=doc.post_id, topics=topics, scores=scores, model_version=MODEL_VERSION)
        for doc, (topics, scores) in zip(body.documents, extracted)
    ]
    log.info("extracted topics for %d documents in %.2fs", len(results), time.time() - started)
    return ExtractResponse(results=results)
