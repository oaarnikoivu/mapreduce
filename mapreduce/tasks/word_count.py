import re

from mapreduce.models.key_value import KeyValue


def map(filename: str, contents: str) -> list[KeyValue]:
    words = re.findall(r"\b[a-zA-Z]+\b", contents)
    return [KeyValue(key=word, value="1") for word in words]


def reduce(key: str, values: list[str]) -> str:
    return str(len(values))
