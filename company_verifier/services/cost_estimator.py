"""Token and cost estimation."""

from __future__ import annotations

from company_verifier.models import AppSettings, CompanyInput, CostEstimate

MODEL_PRICING_PER_MILLION: dict[str, tuple[float, float]] = {
    "openai/gpt-4o-mini": (0.15, 0.60),
    "anthropic/claude-3.5-haiku": (0.80, 4.00),
    "google/gemini-2.0-flash-001": (0.10, 0.40),
    "meta-llama/llama-3.3-70b-instruct": (0.12, 0.30),
}


class CostEstimatorService:
    """Approximate cost estimates from input sizes and model hints."""

    def estimate(self, rows: list[CompanyInput], settings: AppSettings) -> CostEstimate:
        input_chars = sum(len(row.nombre_empresa) + len(row.web_normalized) + 1200 for row in rows)
        input_tokens = max(350, input_chars // 4)
        output_tokens = max(700, min(settings.max_tokens, len(rows) * 280))
        total_tokens = input_tokens + output_tokens
        input_price, output_price = MODEL_PRICING_PER_MILLION.get(settings.model, (0.50, 1.50))
        estimated_cost = (input_tokens / 1_000_000) * input_price + (output_tokens / 1_000_000) * output_price
        return CostEstimate(
            estimated_input_tokens=input_tokens,
            estimated_output_tokens=output_tokens,
            estimated_total_tokens=total_tokens,
            estimated_cost_usd=round(estimated_cost, 4),
        )
