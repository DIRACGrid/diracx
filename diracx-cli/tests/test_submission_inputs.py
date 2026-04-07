"""Tests for diracx.cli._submission.inputs module."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from diracx.cli._submission.inputs import parse_cli_args, parse_input_files, parse_range

# ---------------------------------------------------------------------------
# parse_input_files
# ---------------------------------------------------------------------------


def test_parse_input_files_single_yaml(tmp_path: Path) -> None:
    f = tmp_path / "job.yaml"
    f.write_text("foo: bar\nbaz: 42\n")
    result = parse_input_files([f])
    assert result == [{"foo": "bar", "baz": 42}]


def test_parse_input_files_multi_doc_yaml(tmp_path: Path) -> None:
    f = tmp_path / "jobs.yaml"
    f.write_text("foo: 1\n---\nfoo: 2\n---\nfoo: 3\n")
    result = parse_input_files([f])
    assert result == [{"foo": 1}, {"foo": 2}, {"foo": 3}]


def test_parse_input_files_multiple_files(tmp_path: Path) -> None:
    f1 = tmp_path / "a.yaml"
    f1.write_text("x: 1\n")
    f2 = tmp_path / "b.yaml"
    f2.write_text("x: 2\n---\nx: 3\n")
    result = parse_input_files([f1, f2])
    assert result == [{"x": 1}, {"x": 2}, {"x": 3}]


def test_parse_input_files_json(tmp_path: Path) -> None:
    f = tmp_path / "job.json"
    f.write_text(json.dumps({"key": "value", "num": 7}))
    result = parse_input_files([f])
    assert result == [{"key": "value", "num": 7}]


def test_parse_input_files_empty_list() -> None:
    assert parse_input_files([]) == []


def test_parse_input_files_nonexistent_raises(tmp_path: Path) -> None:
    missing = tmp_path / "does_not_exist.yaml"
    with pytest.raises(FileNotFoundError):
        parse_input_files([missing])


# ---------------------------------------------------------------------------
# parse_range
# ---------------------------------------------------------------------------


def test_parse_range_end_only() -> None:
    param, start, end, step = parse_range("N=10")
    assert param == "N"
    assert start == 0
    assert end == 10
    assert step == 1


def test_parse_range_start_end() -> None:
    param, start, end, step = parse_range("idx=5:20")
    assert param == "idx"
    assert start == 5
    assert end == 20
    assert step == 1


def test_parse_range_start_end_step() -> None:
    param, start, end, step = parse_range("n=0:100:5")
    assert param == "n"
    assert start == 0
    assert end == 100
    assert step == 5


def test_parse_range_no_equals_raises() -> None:
    with pytest.raises(ValueError, match="Invalid range format"):
        parse_range("10")


def test_parse_range_bad_numbers_raises() -> None:
    with pytest.raises(ValueError):
        parse_range("n=abc")


def test_parse_range_negative_step() -> None:
    param, start, end, step = parse_range("n=10:0:-1")
    assert param == "n"
    assert start == 10
    assert end == 0
    assert step == -1


# ---------------------------------------------------------------------------
# parse_cli_args
# ---------------------------------------------------------------------------

CWL_STRING_INPUT = [{"id": "message", "type": "string"}]
CWL_INT_INPUT = [{"id": "count", "type": "int"}]
CWL_FLOAT_INPUT = [{"id": "threshold", "type": "float"}]
CWL_BOOL_INPUT = [{"id": "verbose", "type": "boolean"}]
CWL_FILE_INPUT = [{"id": "infile", "type": "File"}]
CWL_FILE_ARRAY_INPUT = [{"id": "infiles", "type": "File[]"}]
CWL_MULTI_INPUT = [
    {"id": "name", "type": "string"},
    {"id": "count", "type": "int"},
]


def test_parse_cli_args_string() -> None:
    result = parse_cli_args(CWL_STRING_INPUT, ["--message", "hello"])
    assert result == {"message": "hello"}


def test_parse_cli_args_int() -> None:
    result = parse_cli_args(CWL_INT_INPUT, ["--count", "42"])
    assert result == {"count": 42}


def test_parse_cli_args_float() -> None:
    result = parse_cli_args(CWL_FLOAT_INPUT, ["--threshold", "0.75"])
    assert result["threshold"] == pytest.approx(0.75)


def test_parse_cli_args_boolean_flag() -> None:
    result = parse_cli_args(CWL_BOOL_INPUT, ["--verbose"])
    assert result == {"verbose": True}


def test_parse_cli_args_boolean_absent() -> None:
    result = parse_cli_args(CWL_BOOL_INPUT, [])
    assert result == {}


def test_parse_cli_args_file() -> None:
    result = parse_cli_args(CWL_FILE_INPUT, ["--infile", "/data/input.txt"])
    assert result == {"infile": {"class": "File", "path": "/data/input.txt"}}


def test_parse_cli_args_file_array() -> None:
    result = parse_cli_args(
        CWL_FILE_ARRAY_INPUT,
        ["--infiles", "/a.txt", "--infiles", "/b.txt"],
    )
    assert result == {
        "infiles": [
            {"class": "File", "path": "/a.txt"},
            {"class": "File", "path": "/b.txt"},
        ]
    }


def test_parse_cli_args_multiple_inputs() -> None:
    result = parse_cli_args(CWL_MULTI_INPUT, ["--name", "test", "--count", "3"])
    assert result == {"name": "test", "count": 3}


def test_parse_cli_args_unknown_arg_raises() -> None:
    with pytest.raises(SystemExit):
        parse_cli_args(CWL_STRING_INPUT, ["--unknown", "value"])


def test_parse_cli_args_empty_args_returns_empty() -> None:
    result = parse_cli_args(CWL_STRING_INPUT, [])
    assert result == {}
