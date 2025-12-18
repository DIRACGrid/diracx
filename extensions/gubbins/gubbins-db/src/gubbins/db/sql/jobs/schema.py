from diracx.db.sql.job.db import JobDBBase
from diracx.db.sql.job.schema import InputData, str255
from sqlalchemy import ForeignKey
from sqlalchemy.orm import Mapped, mapped_column


# GubbinsInputData: Extends InputData by ADDING a column
# NOTE: Simple inheritance works here because we're ADDING a new column,
# not modifying an existing one. This is the preferred approach when possible.
class GubbinsInputData(InputData):
    """Extended InputData table with Adler checksum support"""

    __tablename__ = "InputData"
    __table_args__ = {"extend_existing": True}

    # New column for Adler checksum
    adler_checksum: Mapped[str255] = mapped_column("AdlerChecksum", default="")


# Example of defining a new table
# NOTE: You need to inherit from the DeclarativeBase of the parent DB
class GubbinsInfo(JobDBBase):
    """An extra table with respect to Vanilla diracx JobDB"""

    __tablename__ = "GubbinsInfo"
    __table_args__ = {"extend_existing": True}

    job_id: Mapped[int] = mapped_column(
        "JobID", ForeignKey("Jobs.JobID", ondelete="CASCADE"), primary_key=True
    )
    info: Mapped[str255] = mapped_column("Info", default="", primary_key=True)
