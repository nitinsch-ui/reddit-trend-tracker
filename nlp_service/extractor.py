"""KeyBERT wrapper, kept free of FastAPI so it can be unit-tested with a fake model."""

from __future__ import annotations

import logging

from normalize import normalize_keywords

log = logging.getLogger(__name__)

KEYBERT_PARAMS = {
    "keyphrase_ngram_range": (1, 2),
    "stop_words": "english",
    "top_n": 8,  # over-fetch: normalisation drops noise and duplicates, then keeps 5
    "use_mmr": True,
    "diversity": 0.7,
}


class TopicExtractor:
    def __init__(self, keybert_model, top_n: int = 5, min_score: float = 0.15):
        self.model = keybert_model
        self.top_n = top_n
        self.min_score = min_score

    def _keywords(self, texts: list[str]) -> list[list[tuple[str, float]]]:
        try:
            result = self.model.extract_keywords(texts, **KEYBERT_PARAMS)
        except ValueError:
            # CountVectorizer raises "empty vocabulary" when the texts are all
            # stop words; fall back to one document at a time to isolate them.
            if len(texts) == 1:
                return [[]]
            return [self._keywords([t])[0] for t in texts]
        # KeyBERT returns a flat list for a single document.
        if len(texts) == 1 and (not result or isinstance(result[0], tuple)):
            return [result]
        return result

    def extract(self, texts: list[str]) -> list[tuple[list[str], list[float]]]:
        """Topics and scores per input text. Empty text yields empty lists, never an error."""
        output: list[tuple[list[str], list[float]]] = [([], []) for _ in texts]
        indexed = [(i, t.strip()) for i, t in enumerate(texts) if t and t.strip()]
        if not indexed:
            return output
        keywords = self._keywords([t for _, t in indexed])
        for (i, _), pairs in zip(indexed, keywords):
            output[i] = normalize_keywords(pairs, self.top_n, self.min_score)
        return output
