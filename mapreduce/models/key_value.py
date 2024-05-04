from pydantic import BaseModel


class KeyValue(BaseModel):
    key: str
    value: str
