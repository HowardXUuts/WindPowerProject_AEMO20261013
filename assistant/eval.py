"""Score the assistant against the question set.

    python assistant/eval.py              # recordings only, no credential needed
    python assistant/eval.py --record     # ask the model and save its replies

**The comparison is on results, not on SQL text.** Two correct queries for
"which farms made negative revenue" can differ in join order, column aliases and
whitespace while returning the same rows, so comparing strings would score
style. Each question carries a reference query; the model passes when its query
returns the same values.

Without recordings the model's half is missing, and the run reports that rather
than claiming a score. What it still proves is that every reference query is
valid against the current gold schema — which is what stops this eval rotting
silently as the schema moves.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Final

from guard import GOLD_DIR, UnsafeQuery, run
from provider import Provider, ProviderError, ask, resolve
from qa import system_prompt

LOG: Final = logging.getLogger("assistant.eval")

QUESTIONS: Final = Path(__file__).resolve().parent / "evals" / "questions.json"

# Floats are rounded before comparison: a query that computes a percentage as
# 100.0*a/b and one that computes (a/b)*100 can differ in the last bit, and that
# is not a wrong answer.
PRECISION: Final = 4


@dataclass(frozen=True)
class Scored:
    """One question's outcome."""

    id: str
    status: str  # pass | fail | refused | error | no-recording
    detail: str


def normalise(rows: list[tuple]) -> list[tuple]:
    """Make two result sets comparable: round floats, stringify, sort."""

    def value(item: object) -> object:
        if isinstance(item, float):
            return round(item, PRECISION)
        if isinstance(item, bool) or item is None:
            return item
        if isinstance(item, int):
            return item
        return str(item)

    return sorted((tuple(value(v) for v in row) for row in rows), key=repr)


def score_one(
    question: dict,
    provider: Provider,
    gold_dir: Path,
    record: bool,
    replay: bool,
) -> Scored:
    """Ask one question and compare its answer with the reference."""
    expected = run(question["reference_sql"], gold_dir)

    try:
        reply, _ = ask(
            system_prompt(), question["question"], provider, replay=replay, record=record
        )
    except ProviderError as exc:
        status = "no-recording" if "no recording" in str(exc) else "error"
        return Scored(question["id"], status, str(exc).splitlines()[0][:90])

    try:
        actual = run(reply, gold_dir)
    except UnsafeQuery as exc:
        return Scored(question["id"], "refused", str(exc))

    if normalise(actual.rows) == normalise(expected.rows):
        return Scored(question["id"], "pass", f"{len(actual.rows)} rows")
    return Scored(
        question["id"],
        "fail",
        f"got {len(actual.rows)} rows, expected {len(expected.rows)}",
    )


def evaluate(
    questions: list[dict],
    provider: Provider | None = None,
    gold_dir: Path = GOLD_DIR,
    record: bool = False,
    replay: bool = True,
) -> list[Scored]:
    chosen = provider or resolve()
    return [score_one(q, chosen, gold_dir, record, replay) for q in questions]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--questions", type=Path, default=QUESTIONS)
    parser.add_argument("--gold-dir", type=Path, default=GOLD_DIR)
    parser.add_argument("--provider")
    parser.add_argument("--model")
    parser.add_argument("--record", action="store_true", help="ask the model and save replies")
    parser.add_argument(
        "--require-recordings",
        action="store_true",
        help="fail if any question has no recorded reply",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    questions = json.loads(args.questions.read_text())

    try:
        provider = resolve(args.provider, args.model)
    except ProviderError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    results = evaluate(
        questions, provider, args.gold_dir, record=args.record, replay=not args.record
    )

    width = max(len(r.id) for r in results)
    for r in results:
        print(f"  {r.status.upper():<13} {r.id:<{width}}  {r.detail}")

    counts: dict[str, int] = {}
    for r in results:
        counts[r.status] = counts.get(r.status, 0) + 1
    summary = ", ".join(f"{n} {status}" for status, n in sorted(counts.items()))
    print(f"\n{len(results)} questions against {provider.model}: {summary}")

    missing = counts.get("no-recording", 0)
    if missing and not args.require_recordings:
        print(
            f"\n{missing} question(s) have no recorded reply, so the model was not scored.\n"
            f"Every reference query is valid against the current gold schema.\n"
            f"To score the model: set {provider.env_var} and run with --record.",
            file=sys.stderr,
        )

    # A failure is a wrong answer or a refused query. A missing recording is
    # only a failure when the caller asked for one.
    bad = counts.get("fail", 0) + counts.get("refused", 0) + counts.get("error", 0)
    if args.require_recordings:
        bad += missing
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
