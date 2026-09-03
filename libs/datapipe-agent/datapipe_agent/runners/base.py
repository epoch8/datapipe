from dataclasses import dataclass
from enum import Enum


class RUN_STATUSES(Enum):
    CREATED = "Created"
    PENDING = "Pending"
    RUNNING = "Running"
    FINISHED = "Finished"
    FAILED = "Failed"
    CANCELED = "Canceled"


@dataclass
class StatusEvent:
    run_id: str
    status: str


@dataclass
class LogEvent:
    run_id: str
    log: str