import pytest

from normalize import normalize_keywords, normalize_topic, singularize


@pytest.mark.parametrize("word, expected", [
    ("floods", "flood"), ("countries", "country"), ("news", "news"), ("crisis", "crisis"),
    ("glass", "glass"), ("virus", "virus"), ("gas", "gas"), ("bees", "bee"),
])
def test_singularize(word, expected):
    assert singularize(word) == expected


def test_normalize_topic_canonicalises():
    assert normalize_topic("  Monsoon   FLOODS ") == "monsoon flood"


@pytest.mark.parametrize("phrase", ["til", "new", "2024", "til 2024", "  ", "ab"])
def test_noise_is_dropped(phrase):
    assert normalize_topic(phrase) is None


def test_word_inside_a_chosen_phrase_is_dropped():
    topics, _ = normalize_keywords([("medieval libraries", 0.8), ("library", 0.7), ("monks", 0.6)])
    assert topics == ["medieval library", "monk"]


def test_normalize_keywords_dedups_filters_and_caps():
    pairs = [("floods", 0.8), ("flood", 0.7), ("til", 0.6), ("kerala", 0.5), ("weak", 0.05),
             ("monsoon rain", 0.4), ("rain", 0.4), ("dam", 0.3), ("river", 0.3), ("extra", 0.3)]
    topics, scores = normalize_keywords(pairs, top_n=5, min_score=0.15)
    assert topics == ["flood", "kerala", "monsoon rain", "dam", "river"]
    assert scores[0] == 0.8
    assert len(topics) == len(scores) == 5
