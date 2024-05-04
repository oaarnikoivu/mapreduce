from pydantic import BaseModel

from .status import Status


class Task(BaseModel):
    id: int
    type: str
    name: str
    status: Status

    class Config:
        use_enum_values = True
