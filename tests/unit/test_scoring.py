import math

import pytest

from scoring import is_trending, trend_score


def test_formula_weights():
    # 10 posts in 5 min -> velocity 2; engagement log(99 + 1); ratio 0.8
    expected = 2 * 0.5 + math.log(100) * 0.3 + 0.8 * 0.2
    assert trend_score(10, 99, 0.8) == pytest.approx(expected)


def test_log_bounds_viral_engagement():
    # 50,000 upvotes add only ~3.2 points: log compression keeps engagement bounded...
    one_viral_post = trend_score(post_count=1, total_score=50_000, avg_upvote_ratio=0.9)
    assert one_viral_post < 4
    # ...but it can still outscore a real burst, so the min_posts guard is what
    # actually keeps a single viral post from being flagged as a trend.
    burst = trend_score(post_count=12, total_score=60, avg_upvote_ratio=0.9)
    assert one_viral_post > burst
    assert not is_trending(1, one_viral_post, min_posts=2, threshold=1.0)
    assert is_trending(12, burst, min_posts=2, threshold=1.0)


def test_negative_scores_and_missing_ratio_are_safe():
    assert trend_score(2, -5, None) == pytest.approx(0.2)


def test_min_posts_guard():
    assert not is_trending(1, 99.0, min_posts=2, threshold=1.0)
    assert is_trending(2, 1.0, min_posts=2, threshold=1.0)
    assert not is_trending(5, 0.9, min_posts=2, threshold=1.0)
