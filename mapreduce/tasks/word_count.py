import re


def map(filename: str, contents: str) -> list[dict[str, str]]:
    words = re.findall(r"\b[a-zA-Z]+\b", contents)

    kva: list[dict[str, str]] = []
    for word in words:
        kv = {word: "1"}
        kva.append(kv)
    return kva


def reduce(key: str, values: list[str]) -> str:
    return str(len(values))