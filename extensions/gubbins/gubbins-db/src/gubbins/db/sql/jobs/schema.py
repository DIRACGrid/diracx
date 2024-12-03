from diracx.db.sql.job.db import JobDBBase
from diracx.db.sql.utils import Column
from sqlalchemy import (
    ForeignKey,
    Integer,
    String,
)


# You need to inherit from the declarative_base of the parent DB
class GubbinsInfo(JobDBBase):
    """An extra table with respect to Vanilla diracx JobDB"""

    __tablename__ = "GubbinsJobs"

    job_id = Column(
        "JobID", Integer, ForeignKey("Jobs.JobID", ondelete="CASCADE"), primary_key=True
    )
    info = Column("Info", String(255), default="", primary_key=True)
