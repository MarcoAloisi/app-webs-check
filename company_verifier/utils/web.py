"""Normalization helpers for company websites."""

from __future__ import annotations

import hashlib
import re
from urllib.parse import urlparse


def normalize_column_name(value: str) -> str:
    normalized = re.sub(r"\s+", "_", value.strip().lower())
    normalized = normalized.replace("á", "a").replace("é", "e").replace("í", "i").replace("ó", "o").replace("ú", "u")
    return normalized



def normalize_url(raw_url: str) -> str:
    url = (raw_url or "").strip()
    if not url:
        return ""
    if not re.match(r"^https?://", url, re.IGNORECASE):
        url = f"https://{url}"
    parsed = urlparse(url)
    host = parsed.netloc.lower().strip()
    if host.startswith("www."):
        host = host[4:]
    path = parsed.path.rstrip("/")
    return f"{parsed.scheme.lower()}://{host}{path}" if path else f"{parsed.scheme.lower()}://{host}"



def extract_domain(url: str) -> str:
    parsed = urlparse(url)
    host = parsed.netloc.lower().strip()
    if host.startswith("www."):
        host = host[4:]
    return host



def is_probably_valid_url(url: str) -> bool:
    parsed = urlparse(url)
    return bool(parsed.scheme and parsed.netloc and "." in parsed.netloc)



def build_record_hash(company_name: str, normalized_url: str) -> str:
    digest = hashlib.sha256(f"{company_name.strip().lower()}|{normalized_url}".encode("utf-8")).hexdigest()
    return digest[:16]



def extract_text_snippet(text: str, limit: int = 400) -> str:
    squashed = re.sub(r"\s+", " ", text or "").strip()
    return squashed[:limit]
