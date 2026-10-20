"""Trend score: (velocity x 0.5) + (log(engagement + 1) x 0.3) + (upvote_ratio x 0.2).

velocity    posts per minute in the window - the rate of growth is the primary signal
engagement  summed post score, log-compressed so one viral post cannot dominate
consensus   average upvote ratio; low weight, because a controversial topic
            that is moving fast is still trending
"""

from __future__ import annotations

import math

VELOCITY_WEIGHT = 0.5
ENGAGEMENT_WEIGHT = 0.3
CONSENSUS_WEIGHT = 0.2
CONTROVERSIAL_RATIO = 0.5


def trend_score(post_count: int, total_score: int, avg_upvote_ratio: float | None, window_minutes: float = 5) -> float:
    velocity = post_count / window_minutes
    engagement = math.log(max(total_score, 0) + 1)
    return VELOCITY_WEIGHT * velocity + ENGAGEMENT_WEIGHT * engagement + CONSENSUS_WEIGHT * (avg_upvote_ratio or 0.0)


def is_trending(post_count: int, score: float, min_posts: int, threshold: float) -> bool:
    """min_posts guards against a single viral post looking like a trend."""
    return post_count >= min_posts and score >= threshold


def add_trend_columns(df, window_minutes: float, min_posts: int, threshold: float):
    """Spark version of the functions above, applied to Job 2's window aggregates."""
    from pyspark.sql import functions as F

    velocity = F.col("post_count") / F.lit(float(window_minutes))
    engagement = F.log1p(F.greatest(F.col("total_score"), F.lit(0)).cast("double"))
    score = (F.lit(VELOCITY_WEIGHT) * velocity
             + F.lit(ENGAGEMENT_WEIGHT) * engagement
             + F.lit(CONSENSUS_WEIGHT) * F.coalesce(F.col("avg_upvote_ratio"), F.lit(0.0)))
    return (df.withColumn("velocity", velocity)
              .withColumn("trend_score", score)
              .withColumn("is_trending", (F.col("post_count") >= min_posts) & (F.col("trend_score") >= threshold))
              .withColumn("is_controversial", F.col("avg_upvote_ratio") < CONTROVERSIAL_RATIO))
