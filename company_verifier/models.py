"""Shared Pydantic models for verification flows."""

from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel, Field, HttpUrl, field_validator, model_validator


class StepStatus(str, Enum):
    COMPLETED = "completed"
    FAILED = "failed"
    NOT_VERIFIABLE = "not_verifiable"


class TernaryAnswer(str, Enum):
    YES = "si"
    NO = "no"
    UNDETERMINED = "indeterminado"


class LegitimacyAnswer(str, Enum):
    YES = "si"
    NO = "no"
    SUSPICIOUS = "sospechosa"


class RiskLevel(str, Enum):
    LOW = "bajo"
    MEDIUM = "medio"
    HIGH = "alto"


class BinaryAnswer(str, Enum):
    YES = "si"
    NO = "no"


class ProcessingStatus(str, Enum):
    PENDING = "pending"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


class ValidationIssue(BaseModel):
    severity: str
    message: str
    row_numbers: list[int] = Field(default_factory=list)


class CompanyInput(BaseModel):
    row_number: int
    nombre_empresa: str = Field(min_length=1)
    web: str = Field(min_length=1)
    web_normalized: str = Field(min_length=1)
    domain_normalized: str = Field(min_length=1)
    record_hash: str = Field(min_length=8)
    processing_status: ProcessingStatus = ProcessingStatus.PENDING


class VerificationStepResult(BaseModel):
    step_number: int = Field(ge=1, le=7)
    name: str
    status: StepStatus
    finding: str
    evidence: list[str] = Field(default_factory=list)
    sources: list[str] = Field(default_factory=list)


class CompanyVerificationResult(BaseModel):
    nombre_empresa: str
    web_input: str
    web_verificada: str | None = None
    existe: TernaryAnswer = TernaryAnswer.UNDETERMINED
    operativa: TernaryAnswer = TernaryAnswer.UNDETERMINED
    absorbida_adquirida: BinaryAnswer = BinaryAnswer.NO
    rebranded: BinaryAnswer = BinaryAnswer.NO
    legitima: LegitimacyAnswer = LegitimacyAnswer.SUSPICIOUS
    riesgo_fraude: RiskLevel = RiskLevel.MEDIUM
    tipologia_riesgo: list[str] = Field(default_factory=list)
    score_confianza: int = Field(ge=0, le=100, default=0)
    pasos_verificados: list[VerificationStepResult] = Field(default_factory=list, min_length=7, max_length=7)
    justificacion_detallada: str = Field(min_length=20)
    fuentes: list[str] = Field(default_factory=list)
    banderas_rojas: list[str] = Field(default_factory=list)
    banderas_verdes: list[str] = Field(default_factory=list)
    requiere_revision_manual: bool = True
    prompt_enviado: str = ""
    respuesta_llm_cruda: str = ""
    processing_status: ProcessingStatus = ProcessingStatus.COMPLETED

    @field_validator("score_confianza", mode="before")
    @classmethod
    def _coerce_score(cls, value: Any) -> int:
        if value is None:
            return 0
        if isinstance(value, str):
            value = value.strip().replace("%", "")
            if not value:
                return 0
        return max(0, min(100, int(float(value))))

    @model_validator(mode="after")
    def _ensure_manual_review(self) -> "CompanyVerificationResult":
        if self.score_confianza < 70:
            self.requiere_revision_manual = True
        if any(step.status != StepStatus.COMPLETED for step in self.pasos_verificados):
            self.requiere_revision_manual = True
        return self


class VerificationRunMetrics(BaseModel):
    started_at: float | None = None
    finished_at: float | None = None
    processed_rows: int = 0
    total_rows: int = 0
    completed_rows: int = 0
    failed_rows: int = 0
    batches_completed: int = 0
    estimated_cost_usd: float = 0.0
    accumulated_row_seconds: float = 0.0


class AppSettings(BaseModel):
    model: str
    fallback_model: str | None = None
    temperature: float = Field(default=0.2, ge=0.0, le=1.0)
    max_tokens: int = Field(default=1800, ge=500, le=4000)
    batch_size: int = Field(default=30, ge=30, le=50)
    parallel_workers: int = Field(default=10, ge=1, le=20)
    manual_review_threshold: int = Field(default=70, ge=1, le=100)
    enable_web_search: bool = False
    web_search_engine: str = "auto"
    web_search_max_results: int = Field(default=5, ge=1, le=25)
    web_search_max_total_results: int | None = Field(default=None, ge=1, le=200)
    web_search_context_size: str = "medium"
    web_search_allowed_domains: list[str] = Field(default_factory=list)
    web_search_excluded_domains: list[str] = Field(default_factory=list)
    checkpoint_interval: int = Field(default=500, ge=50)


class UploadValidationResult(BaseModel):
    rows: list[CompanyInput]
    issues: list[ValidationIssue] = Field(default_factory=list)
    duplicates_removed: int = 0
    encoding_used: str = "utf-8"
    is_checkpoint_file: bool = False


class LlmEnvelope(BaseModel):
    model: str
    prompt: str
    raw_response: str
    parsed_json: dict[str, Any] | None = None
    used_web_search: bool = False
    provider_metadata: dict[str, Any] = Field(default_factory=dict)


class CostEstimate(BaseModel):
    estimated_input_tokens: int
    estimated_output_tokens: int
    estimated_total_tokens: int
    estimated_cost_usd: float


class CheckpointPayload(BaseModel):
    created_at: str
    settings: AppSettings
    metrics: VerificationRunMetrics
    rows: list[dict[str, Any]]
    results: list[dict[str, Any]]


__all__ = [
    "AppSettings",
    "BinaryAnswer",
    "CheckpointPayload",
    "CompanyInput",
    "CompanyVerificationResult",
    "CostEstimate",
    "LegitimacyAnswer",
    "LlmEnvelope",
    "ProcessingStatus",
    "RiskLevel",
    "StepStatus",
    "TernaryAnswer",
    "UploadValidationResult",
    "ValidationIssue",
    "VerificationRunMetrics",
    "VerificationStepResult",
]
