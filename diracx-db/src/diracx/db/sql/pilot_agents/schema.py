from __future__ import annotations

from sqlalchemy import (
    DateTime,
    Double,
    Index,
    Integer,
    String,
    Text,
)
from sqlalchemy.orm import declarative_base

from ..utils import Column, EnumBackedBool, NullColumn

PilotAgentsDBBase = declarative_base()


class PilotAgents(PilotAgentsDBBase):
    __tablename__ = "PilotAgents"

    pilot_id = Column("PilotID", Integer, autoincrement=True, primary_key=True)
    initial_job_id = Column("InitialJobID", Integer, default=0)
    current_job_id = Column("CurrentJobID", Integer, default=0)
    pilot_job_reference = Column("PilotJobReference", String(255), default="Unknown")
    pilot_stamp = Column("PilotStamp", String(32), default="")
    destination_site = Column("DestinationSite", String(128), default="NotAssigned")
    queue = Column("Queue", String(128), default="Unknown")
    grid_site = Column("GridSite", String(128), default="Unknown")
    vo = Column("VO", String(64))
    grid_type = Column("GridType", String(32), default="LCG")
    benchmark = Column("BenchMark", Double, default=0.0)
    submission_time = NullColumn("SubmissionTime", DateTime)
    last_update_time = NullColumn("LastUpdateTime", DateTime)
    status = Column("Status", String(32), default="Unknown")
    status_reason = Column("StatusReason", String(255), default="Unknown")
    accounting_sent = Column("AccountingSent", EnumBackedBool(), default=False)

    __table_args__ = (
        Index("PilotJobReference", "PilotJobReference"),
        Index("Status", "Status"),
        Index("Statuskey", "GridSite", "DestinationSite", "Status"),
    )


class JobToPilotMapping(PilotAgentsDBBase):
    __tablename__ = "JobToPilotMapping"

    pilot_id = Column("PilotID", Integer, primary_key=True)
    job_id = Column("JobID", Integer, primary_key=True)
    start_time = Column("StartTime", DateTime)

    __table_args__ = (Index("JobID", "JobID"), Index("PilotID", "PilotID"))


class PilotOutput(PilotAgentsDBBase):
    __tablename__ = "PilotOutput"

    pilot_id = Column("PilotID", Integer, primary_key=True)
    std_output = Column("StdOutput", Text)
    std_error = Column("StdError", Text)


class PilotsHistorySummary(PilotAgentsDBBase):
    __tablename__ = "PilotsHistorySummary"
    grid_site = Column("GridSite", String(128), primary_key=True)
    destination_site = Column("DestinationSite", String(128), primary_key=True)
    status = Column("Status", String(32), primary_key=True)
    vo = Column("VO", String(64), primary_key=True)
    pilot_count = Column("PilotCount", Integer, default=0)


# TODO:

# DELIMITER //

# CREATE TRIGGER trg_PilotAgents_insert
# AFTER INSERT ON PilotAgents
# FOR EACH ROW
# BEGIN
#   INSERT INTO PilotsHistorySummary (GridSite, DestinationSite, Status, VO, PilotCount)
#   VALUES (NEW.GridSite, NEW.DestinationSite, NEW.Status, NEW.VO, 1)
#   ON DUPLICATE KEY UPDATE PilotCount = PilotCount + 1;
# END;
# //

# CREATE TRIGGER trg_PilotAgents_delete
# AFTER DELETE ON PilotAgents
# FOR EACH ROW
# BEGIN
#   UPDATE PilotsHistorySummary
#   SET PilotCount = PilotCount - 1
#   WHERE GridSite = OLD.GridSite
#     AND DestinationSite = OLD.DestinationSite
#     AND Status = OLD.Status
#     AND VO = OLD.VO;

#   -- Remove zero rows
#   DELETE FROM PilotsHistorySummary
#   WHERE PilotCount = 0
#     AND GridSite = OLD.GridSite
#     AND DestinationSite = OLD.DestinationSite
#     AND Status = OLD.Status
#     AND VO = OLD.VO;
# END;
# //

# CREATE TRIGGER trg_PilotAgents_update_status
# AFTER UPDATE ON PilotAgents
# FOR EACH ROW
# BEGIN
#   IF OLD.Status != NEW.Status THEN

#     -- Decrease count from old status
#     UPDATE PilotsHistorySummary
#     SET PilotCount = PilotCount - 1
#     WHERE GridSite = OLD.GridSite
#       AND DestinationSite = OLD.DestinationSite
#       AND Status = OLD.Status
#       AND VO = OLD.VO;

#     -- Delete row if count drops to zero
#     DELETE FROM PilotsHistorySummary WHERE PilotCount = 0;

#     -- Increase count for new status
#     INSERT INTO PilotsHistorySummary (GridSite, DestinationSite, Status, VO, PilotCount)
#     VALUES (NEW.GridSite, NEW.DestinationSite, NEW.Status, NEW.VO, 1)
#     ON DUPLICATE KEY UPDATE PilotCount = PilotCount + 1;

#   END IF;
# END;
# //

# DELIMITER ;
