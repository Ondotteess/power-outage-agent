from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path

from app.normalization.automaton import AutomatonNormalizer, FallbackNormalizer, RegexNormalizer
from app.quality.evaluation import evaluate_normalizer, load_quality_cases


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Evaluate normalization quality fixtures")
    parser.add_argument(
        "--cases",
        default="docs/quality/normalization_cases.json",
        help="Path to normalization fixture JSON",
    )
    return parser.parse_args()


async def main() -> int:
    args = _parse_args()
    cases_path = Path(args.cases)
    cases = load_quality_cases(cases_path)
    automaton = AutomatonNormalizer()
    report = {
        "automaton": await evaluate_normalizer(automaton, cases),
        "automaton_plus_regex": await evaluate_normalizer(
            FallbackNormalizer(
                AutomatonNormalizer(),
                RegexNormalizer(),
                threshold=1.0,
            ),
            cases,
        ),
    }
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if not report["automaton_plus_regex"]["failures"] else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
