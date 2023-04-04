from typing import Annotated
from fastapi import APIRouter, Query
from enum import StrEnum
from pydantic import BaseModel

router = APIRouter(tags=["jobs"])


class JobStatus(StrEnum):
    Running = "Running"
    Stalled = "Stalled"
    Killed = "Killed"


@router.get("/{job_id}")
async def get_single_job(job_id: int):
    return f"This job {job_id}"


@router.delete("/{job_id}")
async def delete_single_job(job_id: int):
    return f"I am deleting {job_id}"


@router.post("/{job_id}/kill")
async def kill_single_job(job_id: int):
    return f"I am killing {job_id}"


@router.get("/{job_id}/status")
async def get_single_job_status(job_id: int) -> JobStatus:
    return JobStatus.Stalled


@router.post("/{job_id}/status")
async def set_single_job_status(job_id: int, status: JobStatus):
    return f"Updating Job {job_id} to {status}"


@router.get("/")
async def get_bulk_jobs() -> list:
    return [
        {"jobID": 1, "Status": JobStatus.Running},
        {"jobID": 2, "Status": JobStatus.Stalled},
    ]


class JobID(BaseModel):
    job_id: int


@router.delete("/")
async def delete_bulk_jobs(job_ids: Annotated[list[int], Query()]):
    return job_ids


@router.post("/kill")
async def kill_bulk_jobs(job_ids: Annotated[list[int], Query()]):
    return job_ids


@router.get("/status")
async def get_bulk_job_status(job_ids: Annotated[list[int], Query()]):
    return [{"job_id": job.job_id, "status": JobStatus.Running} for job in job_ids]


class JobStatusUpdate(BaseModel):
    job_id: int
    status: JobStatus


@router.post("/status")
async def set_bulk_job_status(job_update: list[JobStatusUpdate]):
    return [{"job_id": job.job_id, "status": job.status} for job in job_update]


class JobDefinition(BaseModel):
    owner: str
    group: str
    vo: str
    jdl: str


@router.post("/")
async def submit_bulk_jobs(job_definitions: list[JobDefinition]):
    return list(range(len(job_definitions)))
