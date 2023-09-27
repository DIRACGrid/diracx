from __future__ import annotations

import logging
import secrets

from diracx.api.jobs import create_sandbox, download_sandbox


async def test_upload_download_sandbox(tmp_path, with_cli_login, caplog):
    caplog.set_level(logging.DEBUG)

    input_directory = tmp_path / "input"
    input_directory.mkdir()
    input_files = []

    input_file = input_directory / "input.dat"
    input_file.write_bytes(secrets.token_bytes(512))
    input_files.append(input_file)

    input_file = input_directory / "a" / "b" / "c" / "nested.dat"
    input_file.parent.mkdir(parents=True)
    input_file.write_bytes(secrets.token_bytes(512))
    input_files.append(input_file)

    # Upload the sandbox
    caplog.clear()
    pfn = await create_sandbox(input_files)
    assert has_record(caplog.records, "diracx.api.jobs", "Uploading sandbox for")

    # Uploading the same sandbox again should return the same PFN
    caplog.clear()
    pfn2 = await create_sandbox(input_files)
    assert pfn == pfn2
    assert has_record(caplog.records, "diracx.api.jobs", "already exists in storage")

    # Download the sandbox
    destination = tmp_path / "output"
    await download_sandbox(pfn, destination)
    assert (destination / "input.dat").is_file()
    assert (destination / "nested.dat").is_file()


def has_record(records: list[logging.LogRecord], logger_name: str, message: str):
    for record in records:
        if record.name == logger_name and message in record.message:
            return True
    return False
