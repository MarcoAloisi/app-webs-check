"""Logging utilities for audit trails."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any


def json_log(event: str, **payload: Any) -> str:
    """Serialize a structured log line."""
    document = {
        "timestamp": datetime.now(UTC).isoformat(),
        "event": event,
        **payload,
    }
    return json.dumps(document, ensure_ascii=False)
