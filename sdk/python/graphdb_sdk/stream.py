from __future__ import annotations

import json
from typing import Iterator
from .errors import GraphDBAPIError


class NDJSONStream:
    def __init__(self, response, *, require_done: bool = False, require_success: bool = False) -> None:
        self._response = response
        self._require_done = require_done
        self._require_success = require_success

    def __iter__(self) -> Iterator[dict]:
        done = False
        for line in self._response:
            if not line:
                continue
            text = line.decode("utf-8").strip()
            if text:
                item = json.loads(text)
                done = done or item.get("done") is True
                if self._require_success and item.get("done") is True and item.get("success") is not True:
                    error = item.get("error") or {}
                    raise GraphDBAPIError(200, error.get("code", ""), error.get("message", "Cypher stream failed"), error.get("retryable", False), detail=error.get("detail"))
                yield item
        if self._require_done and not done:
            raise EOFError("stream ended without a done marker")

    def close(self) -> None:
        self._response.close()

    def __enter__(self) -> "NDJSONStream":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()
