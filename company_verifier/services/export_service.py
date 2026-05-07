"""Result export utilities."""

from __future__ import annotations

import io
import json

import pandas as pd

from company_verifier.models import CompanyVerificationResult

COMPLEX_COLUMNS = ["tipologia_riesgo", "pasos_verificados", "fuentes", "banderas_rojas", "banderas_verdes"]
RESULT_EXPORT_COLUMNS = [
    "nombre_empresa",
    "web_input",
    "web_verificada",
    "existe",
    "operativa",
    "absorbida_adquirida",
    "rebranded",
    "legitima",
    "riesgo_fraude",
    "tipologia_riesgo",
    "score_confianza",
    "justificacion_detallada",
    "fuentes",
    "banderas_rojas",
    "banderas_verdes",
    "requiere_revision_manual",
    "processing_status",
]


class ExportService:
    """Flatten results and export them to common formats."""

    def _placeholder_steps(self) -> list[dict[str, object]]:
        return [
            {
                "step_number": step_number,
                "name": f"Paso {step_number}",
                "status": "not_verifiable",
                "finding": "No disponible en el archivo exportado.",
                "evidence": [],
                "sources": [],
            }
            for step_number in range(1, 8)
        ]

    def _coerce_results_payload(self, payload: object) -> list[dict[str, object]]:
        if isinstance(payload, dict):
            if isinstance(payload.get("results"), list):
                return [dict(item) for item in payload["results"] if isinstance(item, dict)]
            if payload.get("nombre_empresa"):
                return [dict(payload)]
        if isinstance(payload, list):
            return [dict(item) for item in payload if isinstance(item, dict)]
        raise ValueError("Formato JSON no soportado. Usa checkpoint JSON, array JSON de resultados o JSONL exportado.")

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
            if not payload.get("pasos_verificados"):
                payload["pasos_verificados"] = self._placeholder_steps()
            results.append(CompanyVerificationResult.model_validate(payload))
        return results

    def from_json_bytes(self, raw_bytes: bytes) -> list[CompanyVerificationResult]:
        payload = json.loads(raw_bytes.decode("utf-8-sig"))
        return self.from_flat_records(self._coerce_results_payload(payload))

    def from_jsonl_bytes(self, raw_bytes: bytes) -> list[CompanyVerificationResult]:
        records: list[dict[str, object]] = []
        for line in raw_bytes.decode("utf-8-sig").splitlines():
            if not line.strip():
                continue
            payload = json.loads(line)
            if not isinstance(payload, dict):
                raise ValueError("El archivo JSONL debe contener un objeto JSON por línea.")
            records.append(payload)
        return self.from_flat_records(records)

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

    def to_results_export_dataframe(self, results: list[CompanyVerificationResult]) -> pd.DataFrame:
        frame = self.to_dataframe(results)
        if frame.empty:
            return frame
        available_columns = [column for column in RESULT_EXPORT_COLUMNS if column in frame.columns]
        return frame[available_columns].copy()

    def to_csv_bytes(self, results: list[CompanyVerificationResult]) -> bytes:
        frame = self.to_results_export_dataframe(results)
        return frame.to_csv(index=False).encode("utf-8-sig")

    def to_excel_bytes(self, results: list[CompanyVerificationResult]) -> bytes:
        frame = self.to_results_export_dataframe(results)
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
            frame.to_excel(writer, sheet_name="resultados", index=False)
        return buffer.getvalue()

    def jsonl_bytes(self, results: list[CompanyVerificationResult]) -> bytes:
        lines = [json.dumps(result.model_dump(mode="json"), ensure_ascii=False) for result in results]
        return "\n".join(lines).encode("utf-8")
