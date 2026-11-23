from __future__ import annotations

import json
from typing import Iterator

from .errors import GraphDBAPIError


class NDJSONStream:
    def __init__(self, response, *, require_done: bool = False) -> None:
        self._response = response
        self._require_done = require_done

    def __iter__(self) -> Iterator[dict]:
        done = False
        for line in self._response:
            if not line:
                continue
            text = line.decode("utf-8").strip()
            if text:
                item = json.loads(text)
                done = done or item.get("done") is True
                if item.get("done") is True and item.get("error"):
                    error = GraphDBAPIError(
                        200, code=item.get("code", ""), message=item.get("message") or item["error"],
                        retryable=bool(item.get("retryable")), detail=item.get("detail"), body=line,
                    )
                    yield item
                    raise error
                yield item
        if self._require_done and not done:
            raise EOFError("stream ended without a done marker")

    def close(self) -> None:
        self._response.close()

    def __enter__(self) -> "NDJSONStream":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()
