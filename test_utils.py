import os
import pytest
from peer import count_words, line_count, sha256_checksum, get_file_metadata


@pytest.fixture(scope="module")
def test_file():
    filename = "test_sample.txt"
    with open(filename, "w") as f:
        f.write("Hello world\nThis is a test file\nLine three")
    yield filename
    os.remove(filename)


def test_count_words(test_file):
    assert count_words(test_file) == 9


def test_line_count(test_file):
    assert line_count(test_file) == 3


def test_sha256_checksum(test_file):
    checksum = sha256_checksum(test_file)
    assert isinstance(checksum, str)
    assert len(checksum) == 64  # SHA-256 hex length


def test_get_file_metadata(test_file):
    meta = get_file_metadata(test_file)
    assert "size_bytes" in meta
    assert "word_count" in meta
    assert "line_count" in meta
    assert "sha256" in meta
    assert meta["word_count"] == 9
    assert meta["line_count"] == 3
