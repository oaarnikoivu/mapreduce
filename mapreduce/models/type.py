from enum import Enum


class Type(str, Enum):
    MAP = "map"
    REDUCE = "reduce"
