"""Bake the sentence-transformer into the image so the service starts offline."""

import os

from sentence_transformers import SentenceTransformer

SentenceTransformer(os.getenv("NLP_MODEL_NAME", "sentence-transformers/all-MiniLM-L6-v2"), device="cpu")
