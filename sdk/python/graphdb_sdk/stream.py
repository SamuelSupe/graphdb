from __future__ import annotations

import json
from typing import Iterator


class NDJSONStream:
    def __init__(self, response) -> None:
        self._response = response

    def __iter__(self) -> Iterator[dict]:
        for line in self._response:
            if not line:
                continue
            text = line.decode("utf-8").strip()
            if text:
                yield json.loads(text)

    def close(self) -> None:
        self._response.close()

    def __enter__(self) -> "NDJSONStream":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()
