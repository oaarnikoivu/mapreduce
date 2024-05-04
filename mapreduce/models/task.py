from pydantic import BaseModel, ConfigDict

from .status import Status


class MapTask(BaseModel):
    model_config = ConfigDict(use_enum_values=True)

    id: int
    name: str
    status: Status


class ReduceTask(BaseModel):
    model_config = ConfigDict(use_enum_values=True)

    partition: int
    status: Status
