"""HTTP client for the NLP service, called from Job 1's foreachBatch on the driver."""

from __future__ import annotations

import logging
import time
from typing import Dict, List, Tuple

log = logging.getLogger(__name__)

TopicMap = Dict[str, Tuple[List[str], List[float], str]]


class NlpServiceError(RuntimeError):
    pass


def extract_topics(docs: List[dict], url: str, chunk_size: int = 100, timeout: float = 120,
                   retries: int = 3, session=None) -> TopicMap:
    """post_id -> (topics, scores, model_version). Raises NlpServiceError once retries are spent."""
    if session is None:
        import requests

        session = requests.Session()
    results: TopicMap = {}
    for start in range(0, len(docs), chunk_size):
        chunk = docs[start:start + chunk_size]
        for attempt in range(1, retries + 1):
            try:
                response = session.post(url, json={"documents": chunk}, timeout=timeout)
                response.raise_for_status()
                for item in response.json()["results"]:
                    results[item["post_id"]] = (item["topics"], item["scores"], item["model_version"])
                break
            except Exception as exc:  # noqa: BLE001 - any failure is retried, then surfaced
                if attempt == retries:
                    raise NlpServiceError(f"NLP service failed after {retries} attempts: {exc}") from exc
                wait = 2 ** attempt
                log.warning("NLP call failed (attempt %d/%d): %s; retrying in %ss", attempt, retries, exc, wait)
                time.sleep(wait)
    return results
