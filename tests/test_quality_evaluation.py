from __future__ import annotations

from pathlib import Path

from app.normalization.automaton import AutomatonNormalizer
from app.quality.evaluation import evaluate_normalizer, load_quality_cases


async def test_quality_fixture_passes_with_automaton_normalizer():
    cases = load_quality_cases(Path("docs/quality/normalization_cases.json"))
    report = await evaluate_normalizer(AutomatonNormalizer(), cases)

    assert report["total"] == 3
    assert report["normalization_rate"] == 1.0
    assert report["event_type_accuracy"] == 1.0
    assert report["address_accuracy"] == 1.0
    assert report["confidence_pass_rate"] == 1.0
    assert report["failures"] == []
