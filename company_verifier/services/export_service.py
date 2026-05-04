"""Result export utilities."""

from __future__ import annotations

import io
import json

import pandas as pd

from company_verifier.models import CompanyVerificationResult

COMPLEX_COLUMNS = ["tipologia_riesgo", "pasos_verificados", "fuentes", "banderas_rojas", "banderas_verdes"]


class ExportService:
    """Flatten results and export them to common formats."""

    def from_flat_records(self, records: list[dict[str, object]]) -> list[CompanyVerificationResult]:
        results: list[CompanyVerificationResult] = []
        for record in records:
            payload = dict(record)
            for column in COMPLEX_COLUMNS:
                value = payload.get(column)
                if isinstance(value, str) and value:
                    try:
                        payload[column] = json.loads(value)
                    except json.JSONDecodeError:
                        payload[column] = []
            if "web_input" not in payload:
                payload["web_input"] = payload.get("web")
            results.append(CompanyVerificationResult.model_validate(payload))
        return results

    def to_dataframe(self, results: list[CompanyVerificationResult]) -> pd.DataFrame:
        rows = []
        for result in results:
            payload = result.model_dump(mode="json")
            for column in COMPLEX_COLUMNS:
                payload[column] = json.dumps(payload.get(column, []), ensure_ascii=False)
            rows.append(payload)
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame(rows)

    def to_csv_bytes(self, results: list[CompanyVerificationResult]) -> bytes:
        frame = self.to_dataframe(results)
        return frame.to_csv(index=False).encode("utf-8-sig")

    def to_excel_bytes(self, results: list[CompanyVerificationResult]) -> bytes:
        frame = self.to_dataframe(results)
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
            frame.to_excel(writer, sheet_name="resultados", index=False)
        return buffer.getvalue()

    def jsonl_bytes(self, results: list[CompanyVerificationResult]) -> bytes:
        lines = [json.dumps(result.model_dump(mode="json"), ensure_ascii=False) for result in results]
        return "\n".join(lines).encode("utf-8")
