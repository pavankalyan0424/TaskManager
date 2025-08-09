"""
Module for task utilities
"""

import hashlib
import os


def count_words(file_path):
    """
    Method to count words in a file
    :param file_path:
    :return:
    """
    try:
        with open(file_path, "r", encoding="utf-8", errors="replace") as f:
            return len(f.read().split())
    except FileNotFoundError:
        return 0


def sha256_checksum(file_path):
    """
    Method to calculate sha256 of a file
    :param file_path:
    :return:
    """
    try:
        h = hashlib.sha256()
        with open(file_path, "rb") as f:
            for block in iter(lambda: f.read(4096), b""):
                h.update(block)
        return h.hexdigest()
    except FileNotFoundError:
        return "File not found"


def line_count(file_path):
    """
    Method to count lines in a file
    :param file_path:
    :return:
    """
    try:
        with open(file_path, "r", encoding="utf-8", errors="replace") as f:
            return sum(1 for _ in f)
    except FileNotFoundError:
        return 0


def file_count(directory_path):
    """
    Method to count files in a directory
    :param directory_path:
    :return:
    """
    if not os.path.isdir(directory_path):
        return "Path is not a directory"
    return len(os.listdir(directory_path))


def get_file_metadata(file_path):
    """
    Method to get file metadata
    :param file_path:
    :return:
    """
    try:
        size_bytes = os.path.getsize(file_path)
        return {
            "size_bytes": size_bytes,
            "word_count": count_words(file_path),
            "line_count": line_count(file_path),
            "sha256": sha256_checksum(file_path),
        }
    except Exception as e:
        return {"error": str(e)}


TASKS = {
    "word_count": count_words,
    "sha256": sha256_checksum,
    "line_count": line_count,
    "file_count": file_count,
    "file_metadata": get_file_metadata,
}
