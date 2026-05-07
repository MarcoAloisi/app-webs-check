"""Website evidence collection utilities."""

from __future__ import annotations

import re
import socket
import ssl
from typing import Any
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from company_verifier.config import DEFAULT_TIMEOUT_SECONDS, USER_AGENT
from company_verifier.utils.web import extract_text_snippet

PARKED_PATTERNS = [
    "domain for sale",
    "buy this domain",
    "sedo",
    "parking",
    "this domain is parked",
]
VAT_PATTERNS = [r"\b(?:vat|nif|cif|iva)[:\s#-]*([a-z0-9\-\.]{5,20})", r"\b([a-z]{1,2}\d{8,12})\b"]
PHONE_PATTERN = r"(?:\+?\d[\d\s\-\(\)]{7,}\d)"
EMAIL_PATTERN = r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}"
ADDRESS_PATTERN = r"\b(?:calle|avenida|av\.?|road|street|st\.?|plaza|paseo|poligono)\b.{0,80}"


class WebEvidenceService:
    """Collects HTTP and content evidence from a company website."""

    def __init__(self) -> None:
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": USER_AGENT})

    def collect(self, company_name: str, website: str) -> dict[str, Any]:
        evidence: dict[str, Any] = {
            "company_name": company_name,
            "input_url": website,
            "final_url": website,
            "status_code": None,
            "redirect_chain": [],
            "ssl_valid": None,
            "ssl_error": None,
            "domain_expired": None,
            "whois_registration_date": None,
            "html_title": None,
            "meta_description": None,
            "text_snippet": "",
            "contact_emails": [],
            "contact_phones": [],
            "legal_identifiers": [],
            "address_candidates": [],
            "company_name_match": False,
            "parked_domain": False,
            "empty_site": False,
            "error": None,
            "source_urls": [website],
        }
        try:
            response = self._session.get(website, timeout=DEFAULT_TIMEOUT_SECONDS, allow_redirects=True)
            evidence["status_code"] = response.status_code
            evidence["final_url"] = response.url
            evidence["redirect_chain"] = [item.url for item in response.history]
            evidence["source_urls"] = [*evidence["redirect_chain"], response.url]
            evidence["ssl_valid"], evidence["ssl_error"] = self._validate_ssl(response.url)
            self._populate_content_fields(evidence, response.text, response.url)
        except requests.RequestException as exc:
            evidence["error"] = str(exc)
            evidence["ssl_valid"], evidence["ssl_error"] = self._validate_ssl(website)
        except Exception as exc:  # noqa: BLE001
            evidence["error"] = f"Fallo inesperado al recopilar evidencia web: {exc}"
            evidence["ssl_valid"], evidence["ssl_error"] = self._validate_ssl(website)
        return evidence

    def _populate_content_fields(self, evidence: dict[str, Any], html: str, base_url: str) -> None:
        soup = BeautifulSoup(html, "html.parser")
        title = soup.title.string.strip() if soup.title and soup.title.string else ""
        meta_tag = soup.find("meta", attrs={"name": re.compile("description", re.I)})
        meta_description = meta_tag.get("content", "").strip() if meta_tag else ""
        text = soup.get_text(" ", strip=True)
        snippet = extract_text_snippet(text, limit=800)
        evidence["html_title"] = title or None
        evidence["meta_description"] = meta_description or None
        evidence["text_snippet"] = snippet
        evidence["contact_emails"] = sorted(set(re.findall(EMAIL_PATTERN, html, flags=re.IGNORECASE)))[:5]
        evidence["contact_phones"] = sorted(set(re.findall(PHONE_PATTERN, text)))[:5]
        legal_ids: list[str] = []
        for pattern in VAT_PATTERNS:
            legal_ids.extend(match.group(1) if hasattr(match, "group") else match for match in re.finditer(pattern, text, flags=re.IGNORECASE))
        evidence["legal_identifiers"] = sorted(set(str(item).strip() for item in legal_ids if str(item).strip()))[:5]
        addresses = re.findall(ADDRESS_PATTERN, text, flags=re.IGNORECASE)
        evidence["address_candidates"] = sorted(set(addresses))[:3]
        haystack = f"{title} {meta_description} {snippet}".lower()
        company_tokens = [token for token in re.split(r"\W+", evidence["company_name"].lower()) if len(token) > 2]
        evidence["company_name_match"] = bool(company_tokens) and sum(token in haystack for token in company_tokens) >= max(1, min(2, len(company_tokens)))
        parked = any(pattern in haystack for pattern in PARKED_PATTERNS)
        evidence["parked_domain"] = parked
        evidence["empty_site"] = len(snippet) < 80
        for link in soup.find_all("a", href=True):
            href = link["href"].strip()
            if href:
                evidence["source_urls"].append(urljoin(base_url, href))
        evidence["source_urls"] = list(dict.fromkeys(evidence["source_urls"]))[:20]

    def _validate_ssl(self, url: str) -> tuple[bool | None, str | None]:
        parsed = urlparse(url)
        host = parsed.hostname
        if not host:
            return None, "hostname ausente"
        try:
            context = ssl.create_default_context()
            with socket.create_connection((host, 443), timeout=8) as sock:
                with context.wrap_socket(sock, server_hostname=host):
                    return True, None
        except Exception as exc:  # noqa: BLE001
            return False, str(exc)
