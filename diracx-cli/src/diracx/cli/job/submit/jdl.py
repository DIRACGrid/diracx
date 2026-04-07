from __future__ import annotations

__all__: list[str] = []

from typer import FileText

from diracx.client.aio import AsyncDiracClient

from . import app


@app.async_command(
    help="""Submit jobs in JDL format.

JDL is one or more JDL file paths.

Examples:
  dirac job submit jdl job.jdl
  dirac job submit jdl job1.jdl job2.jdl
""",
)
async def jdl(jdl: list[FileText]):
    """Submit jobs in JDL format."""
    async with AsyncDiracClient() as api:
        jobs = await api.jobs.submit_jdl_jobs([x.read() for x in jdl])
    print(
        f"Inserted {len(jobs)} jobs with ids: {','.join(map(str, (job.job_id for job in jobs)))}"
    )
