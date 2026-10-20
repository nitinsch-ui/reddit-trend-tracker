"""Text cleaning shared by Spark Job 1 (as column expressions) and the tests (as plain Python).

The regexes are deliberately simple enough to behave identically in Java
(Spark's regexp_replace) and Python's re module.
"""

from __future__ import annotations

import re

URL_PATTERN = r"(https?://|www\.)\S+"
NON_TEXT_PATTERN = r"[^a-z0-9\s]"
WHITESPACE_PATTERN = r"\s+"
MAX_TEXT_CHARS = 2000  # titles carry the signal; long self-posts only slow KeyBERT


def clean_text(title: str | None, body: str | None) -> str:
    text = f"{title or ''} {body or ''}".lower()
    text = re.sub(URL_PATTERN, " ", text)
    text = re.sub(NON_TEXT_PATTERN, " ", text)
    text = re.sub(WHITESPACE_PATTERN, " ", text).strip()
    return text[:MAX_TEXT_CHARS]


def clean_text_column(title_col, body_col):
    from pyspark.sql import functions as F

    text = F.lower(F.concat_ws(" ", F.coalesce(title_col, F.lit("")), F.coalesce(body_col, F.lit(""))))
    text = F.regexp_replace(text, URL_PATTERN, " ")
    text = F.regexp_replace(text, NON_TEXT_PATTERN, " ")
    text = F.trim(F.regexp_replace(text, WHITESPACE_PATTERN, " "))
    return F.substring(text, 1, MAX_TEXT_CHARS)
