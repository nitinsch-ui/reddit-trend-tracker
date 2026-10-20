from extractor import TopicExtractor


class FakeKeyBERT:
    """Mimics KeyBERT's return shapes: flat list for one doc, list of lists for many."""

    def __init__(self, fail_on_batch=False):
        self.calls = []
        self.fail_on_batch = fail_on_batch

    def extract_keywords(self, docs, **params):
        self.calls.append(list(docs))
        if self.fail_on_batch and len(docs) > 1:
            raise ValueError("empty vocabulary; perhaps the documents only contain stop words")
        if any(d == "the and of" for d in docs) and len(docs) == 1:
            raise ValueError("empty vocabulary")
        results = [[(w, 0.9 - i * 0.1) for i, w in enumerate(d.split()[:3])] for d in docs]
        return results[0] if len(docs) == 1 else results


def test_batch_and_empty_text():
    model = FakeKeyBERT()
    out = TopicExtractor(model).extract(["floods kerala rain", "", "volcano ash cloud"])
    assert out[0][0] == ["flood", "kerala", "rain"]
    assert out[1] == ([], [])
    assert out[2][0] == ["volcano", "ash", "cloud"]
    assert model.calls == [["floods kerala rain", "volcano ash cloud"]]  # empty text never sent


def test_single_document_shape():
    out = TopicExtractor(FakeKeyBERT()).extract(["bees honey"])
    assert out == [(["bee", "honey"], [0.9, 0.8])]


def test_empty_vocabulary_falls_back_per_document():
    out = TopicExtractor(FakeKeyBERT(fail_on_batch=True)).extract(["the and of", "roman concrete"])
    assert out[0] == ([], [])
    assert out[1][0] == ["roman", "concrete"]


def test_all_empty_skips_model():
    model = FakeKeyBERT()
    assert TopicExtractor(model).extract(["", "  "]) == [([], []), ([], [])]
    assert model.calls == []
