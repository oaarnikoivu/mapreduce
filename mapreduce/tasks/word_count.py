import re

from mapreduce.models.key_value import KeyValue


def map(filename: str, contents: str) -> list[KeyValue]:
    words = re.findall(r"\b[a-zA-Z]+\b", contents)

    kva: list[dict[str, str]] = []
    for word in words:
        kv = KeyValue(key=word, value="1")
        kva.append(kv)
    return kva


def reduce(key: str, values: list[str]) -> str:
    return str(len(values))
