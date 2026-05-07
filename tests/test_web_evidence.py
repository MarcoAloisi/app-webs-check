from __future__ import annotations

from company_verifier.services.web_evidence import WebEvidenceService


class _BoomSession:
    def get(self, *args, **kwargs):  # noqa: ANN002, ANN003
        raise ValueError("'cookie_settings' does not appear to be an IPv4 or IPv6 address")


def test_web_evidence_collect_handles_unexpected_url_errors() -> None:
    service = WebEvidenceService()
    service._session = _BoomSession()  # type: ignore[assignment]

    result = service.collect("TeamEQ", "https://teameq.com")

    assert result["error"] is not None
    assert "cookie_settings" in result["error"]
    assert result["input_url"] == "https://teameq.com"
