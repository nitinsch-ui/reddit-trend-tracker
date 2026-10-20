from text_clean import MAX_TEXT_CHARS, clean_text


def test_clean_text():
    assert clean_text("Breaking: Floods hit Kerala!!", "See https://example.com/x and www.foo.org now") == \
        "breaking floods hit kerala see and now"


def test_handles_none_and_truncates():
    assert clean_text(None, None) == ""
    assert len(clean_text("a " * 5000, "")) <= MAX_TEXT_CHARS
