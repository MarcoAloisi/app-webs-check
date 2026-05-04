from __future__ import annotations

from company_verifier.services.csv_validation import load_csv_bytes



def test_csv_validation_removes_duplicates_and_invalid_urls() -> None:
    raw = "nombre_empresa,web\nA,example.com\nA,example.com\nB,notaurl\n".encode("utf-8")
    frame, validation = load_csv_bytes(raw)

    assert len(frame) == 1
    assert validation.duplicates_removed == 1
    assert any("URL malformada" in issue.message for issue in validation.issues)
    assert frame.iloc[0]["domain_normalized"] == "example.com"
