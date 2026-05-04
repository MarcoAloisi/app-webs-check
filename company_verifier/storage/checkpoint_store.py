"""Checkpoint serialization and resume helpers."""

from __future__ import annotations

import json
from datetime import UTC, datetime

import pandas as pd

from company_verifier.models import AppSettings, CheckpointPayload, CompanyVerificationResult, VerificationRunMetrics
from company_verifier.services.export_service import ExportService


class CheckpointStore:
    """Build downloadable checkpoint artifacts."""

    def __init__(self) -> None:
        self._export_service = ExportService()

    def build_payload(
        self,
        source_frame: pd.DataFrame,
        results: list[CompanyVerificationResult],
        settings: AppSettings,
        metrics: VerificationRunMetrics,
    ) -> str:
        payload = CheckpointPayload(
            created_at=datetime.now(UTC).isoformat(),
            settings=settings,
            metrics=metrics,
            rows=source_frame.to_dict(orient="records"),
            results=[result.model_dump(mode="json") for result in results],
        )
        return payload.model_dump_json(indent=2)

    def build_checkpoint_csv(self, source_frame: pd.DataFrame, results: list[CompanyVerificationResult]) -> str:
        if source_frame.empty:
            return ""
        export_frame = self._export_service.to_dataframe(results)
        if export_frame.empty:
            return source_frame.to_csv(index=False)
        merged = source_frame.merge(
            export_frame,
            left_on=["nombre_empresa", "web"],
            right_on=["nombre_empresa", "web_input"],
            how="left",
        )
        if "web_input" in merged.columns:
            merged = merged.drop(columns=["web_input"])
        if "processing_status_y" in merged.columns:
            merged["processing_status"] = merged["processing_status_y"].fillna(merged.get("processing_status_x"))
            merged = merged.drop(columns=[column for column in ["processing_status_x", "processing_status_y"] if column in merged.columns])
        return merged.to_csv(index=False)

    def load_payload(self, raw_json: str) -> CheckpointPayload:
        return CheckpointPayload.model_validate(json.loads(raw_json))
