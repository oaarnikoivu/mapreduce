from enum import Enum


class Status(str, Enum):
    READY = "ready"
    RUNNING = "running"
    DONE = "done"
