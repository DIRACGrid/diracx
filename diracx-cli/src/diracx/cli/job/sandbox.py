"""Commands for exploring and retrieving job output sandboxes."""

from __future__ import annotations

__all__: list[str] = []

import io
import sys
import tarfile
from pathlib import Path
from typing import Annotated

import httpx
import typer
import zstandard
from rich.console import Console
from rich.syntax import Syntax
from rich.table import Table

from diracx.client.aio import AsyncDiracClient
from diracx.client.models import SandboxType

from ..utils import AsyncTyper

app = AsyncTyper(help="Output sandbox operations.")


async def _get_output_sb_refs(job_id: int) -> list[str]:
    """Return the list of SB: references for a job's output sandbox."""
    async with AsyncDiracClient() as client:
        refs = await client.jobs.get_job_sandbox(job_id, SandboxType.OUTPUT)
        return [r for r in (refs or []) if r is not None]


async def _download_sandbox_bytes(sb_ref: str) -> bytes:
    """Download a sandbox tar archive and return the raw bytes."""
    async with AsyncDiracClient() as client:
        # Strip SB:SE| prefix — the server accepts just the /S3/... path,
        # and the generated client regex has a typo that rejects "SB:"
        pfn = sb_ref.split("|", 1)[-1] if "|" in sb_ref else sb_ref
        res = await client.jobs.get_sandbox_file(pfn=pfn)
    async with httpx.AsyncClient() as http_client:
        response = await http_client.get(res.url)
        response.raise_for_status()
        return response.content


def _open_tar(data: bytes) -> tarfile.TarFile:
    """Open a (possibly zstd-compressed) tar archive from bytes."""
    fh = io.BytesIO(data)
    magic = fh.read(4)
    fh.seek(0)
    if magic.startswith(b"\x28\xb5\x2f\xfd"):
        dctx = zstandard.ZstdDecompressor()
        decompressed = dctx.decompress(data)
        return tarfile.open(fileobj=io.BytesIO(decompressed), mode="r")
    return tarfile.open(fileobj=fh, mode="r")


def _guess_syntax(filename: str) -> str | None:
    """Return a rich Syntax lexer name from filename, or None."""
    ext = Path(filename).suffix.lower()
    return {
        ".py": "python",
        ".sh": "bash",
        ".yaml": "yaml",
        ".yml": "yaml",
        ".json": "json",
        ".xml": "xml",
        ".log": "text",
        ".txt": "text",
        ".cwl": "yaml",
    }.get(ext)


@app.async_command(name="list")
async def list_files(
    job_id: Annotated[int, typer.Argument(help="Job ID")],
):
    """List files in the output sandbox of a job."""
    sb_refs = await _get_output_sb_refs(job_id)
    if not sb_refs:
        print("No output sandbox found for this job.")
        return

    console = Console()
    for sb_ref in sb_refs:
        data = await _download_sandbox_bytes(sb_ref)
        tf = _open_tar(data)
        table = Table(
            "Name", "Size", title=f"Sandbox: {sb_ref.split('/')[-1].split('.')[0][:12]}"
        )
        for member in sorted(tf.getmembers(), key=lambda m: m.name):
            if member.isfile():
                size = member.size
                if size < 1024:
                    size_str = f"{size} B"
                elif size < 1024 * 1024:
                    size_str = f"{size / 1024:.1f} KB"
                else:
                    size_str = f"{size / (1024 * 1024):.1f} MB"
                table.add_row(member.name, size_str)
        console.print(table)


@app.async_command()
async def peek(
    job_id: Annotated[int, typer.Argument(help="Job ID")],
    filename: Annotated[str, typer.Argument(help="File to display (e.g. stdout.log)")],
    lines: Annotated[
        int, typer.Option("--lines", "-n", help="Number of lines to show (0=all)")
    ] = 50,
):
    """Display the contents of a file from the output sandbox."""
    sb_refs = await _get_output_sb_refs(job_id)
    if not sb_refs:
        print("No output sandbox found for this job.", file=sys.stderr)
        raise typer.Exit(1)

    for sb_ref in sb_refs:
        data = await _download_sandbox_bytes(sb_ref)
        tf = _open_tar(data)
        try:
            member = tf.getmember(filename)
        except KeyError:
            continue

        fobj = tf.extractfile(member)
        if fobj is None:
            print(f"{filename} is not a regular file.", file=sys.stderr)
            raise typer.Exit(1)

        content = fobj.read().decode(errors="replace")
        if lines > 0:
            content_lines = content.splitlines()
            if len(content_lines) > lines:
                content = "\n".join(content_lines[:lines])
                content += f"\n... ({len(content_lines) - lines} more lines)"

        console = Console()
        lexer = _guess_syntax(filename)
        if lexer and lexer != "text":
            console.print(Syntax(content, lexer))
        else:
            console.print(content)
        return

    print(f"File '{filename}' not found in any output sandbox.", file=sys.stderr)
    raise typer.Exit(1)


@app.async_command()
async def get(
    job_id: Annotated[int, typer.Argument(help="Job ID")],
    output_dir: Annotated[
        Path, typer.Option("--output", "-o", help="Directory to extract files into")
    ] = Path("."),
    filename: Annotated[
        str | None, typer.Argument(help="Specific file to extract (default: all)")
    ] = None,
):
    """Download output sandbox files to a local directory."""
    sb_refs = await _get_output_sb_refs(job_id)
    if not sb_refs:
        print("No output sandbox found for this job.", file=sys.stderr)
        raise typer.Exit(1)

    output_dir.mkdir(parents=True, exist_ok=True)
    extracted: list[str] = []

    for sb_ref in sb_refs:
        data = await _download_sandbox_bytes(sb_ref)
        tf = _open_tar(data)

        if filename:
            try:
                member = tf.getmember(filename)
            except KeyError:
                continue
            tf.extract(member, path=output_dir, filter="data")
            extracted.append(member.name)
        else:
            for member in tf.getmembers():
                if member.isfile():
                    tf.extract(member, path=output_dir, filter="data")
                    extracted.append(member.name)

    if not extracted:
        if filename:
            print(
                f"File '{filename}' not found in any output sandbox.", file=sys.stderr
            )
        else:
            print("No files extracted.", file=sys.stderr)
        raise typer.Exit(1)

    for name in extracted:
        print(f"  {output_dir / name}")
