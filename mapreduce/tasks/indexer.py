import re

from mapreduce.models.key_value import KeyValue


def map(filename: str, contents: str) -> list[KeyValue]:
    words = set(re.findall(r"\b[a-zA-Z]+\b", contents))
    return [KeyValue(key=word, value=filename) for word in words]


def reduce(key: str, values: list[str]) -> str:
    values.sort()
    return f"{len(values)} {' '.join(values)}"
