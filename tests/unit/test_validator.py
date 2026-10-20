import pytest

from message import build_post
from validator import CHECK_NAMES, validate

NOW = 1_760_000_000.0
SUBS = ["worldnews", "todayilearned"]


def post(**overrides):
    base = build_post(post_id="t3_abc", title="Flooding in Kerala", subreddit="worldnews",
                      score=10, upvote_ratio=0.9, created_utc=NOW - 60, ingested_at=NOW)
    base.update(overrides)
    return base


def test_valid_post_passes():
    assert validate(post(), SUBS, now=NOW) is None


@pytest.mark.parametrize("overrides, check, reason", [
    ({"score": None}, "required_fields", "missing_field:score"),
    ({"post_id": None}, "required_fields", "missing_field:post_id"),
    ({"subreddit": "politics"}, "subreddit_allowlist", "subreddit_not_allowed"),
    ({"upvote_ratio": 1.5}, "upvote_ratio_range", "invalid_upvote_ratio"),
    ({"upvote_ratio": "0.9"}, "upvote_ratio_range", "invalid_upvote_ratio"),
    ({"created_utc": NOW + 3600}, "timestamp_sanity", "invalid_timestamp"),
    ({"created_utc": NOW - 8 * 24 * 3600}, "timestamp_sanity", "invalid_timestamp"),
    ({"title": "   "}, "non_empty_title", "empty_title"),
])
def test_each_check_rejects(overrides, check, reason):
    failure = validate(post(**overrides), SUBS, now=NOW)
    assert failure is not None
    assert (failure.check, failure.reason) == (check, reason)


def test_subreddit_match_is_case_insensitive():
    assert validate(post(subreddit="WorldNews"), SUBS, now=NOW) is None


def test_bool_is_not_a_valid_ratio():
    assert validate(post(upvote_ratio=True), SUBS, now=NOW).check == "upvote_ratio_range"


def test_five_layer_one_checks():
    assert len(CHECK_NAMES) == 5
