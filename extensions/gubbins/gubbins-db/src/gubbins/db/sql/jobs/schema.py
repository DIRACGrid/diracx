from diracx.db.sql.job.db import JobDBBase
from diracx.db.sql.job.schema import jobid_type, str255
from sqlalchemy.orm import Mapped, mapped_column


# You need to inherit from the DeclarativeBase of the parent DB
class GubbinsInfo(JobDBBase):
    """An extra table with respect to Vanilla diracx JobDB"""

    __tablename__ = "GubbinsJobs"

    job_id: Mapped[jobid_type]
    info: Mapped[str255] = mapped_column("Info", default="", primary_key=True)
