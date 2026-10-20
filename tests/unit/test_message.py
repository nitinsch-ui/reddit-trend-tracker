from types import SimpleNamespace

from message import FIELDS, SCHEMA_VERSION, build_post, from_submission, hash_author


def test_message_has_exactly_the_13_contract_fields():
    msg = build_post(post_id="t3_x", title="t", subreddit="WorldNews", score=1, upvote_ratio=1.0, created_utc=1.0)
    assert tuple(msg) == FIELDS
    assert len(FIELDS) == 13
    assert msg["subreddit"] == "worldnews"
    assert msg["schema_version"] == SCHEMA_VERSION


def test_author_is_hashed_not_stored():
    assert hash_author("spez") != "spez"
    assert hash_author("spez") == hash_author("spez")
    assert len(hash_author("spez")) == 16
    assert hash_author(None) is None


def test_from_submission():
    sub = SimpleNamespace(
        id="abc123", title="Title", selftext="", subreddit=SimpleNamespace(display_name="todayilearned"),
        author=SimpleNamespace(name="someone"), score=42, upvote_ratio=0.87, num_comments=5,
        created_utc=1_700_000_000, permalink="/r/todayilearned/comments/abc123/x/", link_flair_text=None,
    )
    msg = from_submission(sub)
    assert msg["post_id"] == "t3_abc123"
    assert msg["subreddit"] == "todayilearned"
    assert msg["author_hash"] == hash_author("someone")
    assert msg["url"].startswith("https://www.reddit.com/r/todayilearned/")
    assert isinstance(msg["created_utc"], float)


def test_from_submission_deleted_author():
    sub = SimpleNamespace(
        id="d", title="T", selftext="b", subreddit=SimpleNamespace(display_name="worldnews"), author=None,
        score=0, upvote_ratio=0.5, num_comments=0, created_utc=1.0, permalink="/p", link_flair_text="News",
    )
    assert from_submission(sub)["author_hash"] is None
