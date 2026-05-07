from __future__ import annotations

import io

import pandas as pd

from company_verifier.services.csv_validation import load_csv_bytes
from company_verifier.services.csv_validation import extract_completed_results, list_sheet_names, load_tabular_bytes



def test_csv_validation_removes_duplicates_and_invalid_urls() -> None:
    raw = "nombre_empresa,web\nA,example.com\nA,example.com\nB,notaurl\n".encode("utf-8")
    frame, validation = load_csv_bytes(raw)

    assert len(frame) == 1
    assert validation.duplicates_removed == 1
    assert any("URL malformada" in issue.message for issue in validation.issues)
    assert any("Empresas duplicadas: A" in issue.message for issue in validation.issues)
    assert frame.iloc[0]["domain_normalized"] == "example.com"


def test_csv_validation_accepts_semicolon_delimiter() -> None:
    raw = "nombre_empresa;web\nChroniSense Medical;http://chronisense.com\nAgito;http://www.agito.com.tr\n".encode("utf-8")

    frame, validation = load_csv_bytes(raw)

    assert len(frame) == 2
    assert validation.issues == []
    assert list(frame.columns)[:2] == ["nombre_empresa", "web"]


def test_excel_validation_accepts_selected_sheet() -> None:
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        pd.DataFrame({"nombre_empresa": ["A"], "web": ["https://example.com"]}).to_excel(writer, sheet_name="empresas", index=False)
        pd.DataFrame({"foo": ["bar"]}).to_excel(writer, sheet_name="otras", index=False)

    frame, validation = load_tabular_bytes(buffer.getvalue(), "empresas.xlsx", sheet_name="empresas")

    assert len(frame) == 1
    assert validation.issues == []
    assert frame.iloc[0]["domain_normalized"] == "example.com"


def test_excel_sheet_names_are_listed() -> None:
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        pd.DataFrame({"nombre_empresa": ["A"], "web": ["https://example.com"]}).to_excel(writer, sheet_name="uno", index=False)
        pd.DataFrame({"nombre_empresa": ["B"], "web": ["https://example.org"]}).to_excel(writer, sheet_name="dos", index=False)

    assert list_sheet_names(buffer.getvalue(), "empresas.xlsx") == ["uno", "dos"]


def test_excel_checkpoint_restores_completed_rows() -> None:
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        pd.DataFrame(
            {
                "nombre_empresa": ["A"],
                "web": ["https://example.com"],
                "web_verificada": ["https://example.com"],
                "existe": ["si"],
                "operativa": ["si"],
                "legitima": ["si"],
                "riesgo_fraude": ["bajo"],
                "tipologia_riesgo": ["[]"],
                "score_confianza": [95],
                "pasos_verificados": ["[]"],
                "justificacion_detallada": ["Justificación suficientemente larga para pasar validación."],
                "fuentes": ["[]"],
                "banderas_rojas": ["[]"],
                "banderas_verdes": ["[]"],
                "requiere_revision_manual": [False],
                "prompt_enviado": ["{}"],
                "respuesta_llm_cruda": ["{}"],
                "processing_status": ["completed"],
            }
        ).to_excel(writer, sheet_name="resultados", index=False)

    frame, validation = load_tabular_bytes(buffer.getvalue(), "checkpoint.xlsx", sheet_name="resultados")
    completed = extract_completed_results(frame)

    assert validation.is_checkpoint_file is True
    assert len(completed) == 1
    assert completed[0]["processing_status"] == "completed"
