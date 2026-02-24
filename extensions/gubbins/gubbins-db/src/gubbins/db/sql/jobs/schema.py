from __future__ import annotations

from diracx.db.sql.job.db import JobDBBase
from diracx.db.sql.job.schema import str255
from sqlalchemy import ForeignKey
from sqlalchemy.orm import Mapped, mapped_column


# You need to inherit from the DeclarativeBase of the parent DB
class GubbinsInfo(JobDBBase):
    """An extra table with respect to Vanilla diracx JobDB"""

    __tablename__ = "GubbinsJobs"

    job_id: Mapped[int] = mapped_column(
        "JobID", ForeignKey("Jobs.JobID", ondelete="CASCADE"), primary_key=True
    )
    info: Mapped[str255] = mapped_column("Info", default="", primary_key=True)
