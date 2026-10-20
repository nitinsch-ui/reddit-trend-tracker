import pytest

from nlp_client import NlpServiceError, extract_topics


class FakeResponse:
    def __init__(self, payload, status=200):
        self.payload, self.status = payload, status

    def raise_for_status(self):
        if self.status >= 400:
            raise RuntimeError(f"HTTP {self.status}")

    def json(self):
        return self.payload


class FakeSession:
    def __init__(self, failures=0):
        self.failures = failures
        self.requests = []

    def post(self, url, json, timeout):
        self.requests.append(json)
        if self.failures:
            self.failures -= 1
            return FakeResponse({}, status=503)
        return FakeResponse({"results": [
            {"post_id": d["post_id"], "topics": [d["text"]], "scores": [0.5], "model_version": "v"}
            for d in json["documents"]]})


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    monkeypatch.setattr("nlp_client.time.sleep", lambda s: None)


def docs(n):
    return [{"post_id": f"p{i}", "text": f"t{i}"} for i in range(n)]


def test_chunks_requests():
    session = FakeSession()
    result = extract_topics(docs(250), "http://nlp", chunk_size=100, session=session)
    assert [len(r["documents"]) for r in session.requests] == [100, 100, 50]
    assert result["p249"] == (["t249"], [0.5], "v")


def test_retries_then_succeeds():
    session = FakeSession(failures=2)
    assert len(extract_topics(docs(3), "http://nlp", retries=3, session=session)) == 3


def test_raises_after_retries():
    with pytest.raises(NlpServiceError):
        extract_topics(docs(3), "http://nlp", retries=2, session=FakeSession(failures=5))
