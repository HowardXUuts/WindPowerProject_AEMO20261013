"""Scoring for the price forecast, in the stdlib.

PLAN 4.6 names the metrics and, more usefully, says why each one:

- **MAE and RMSE, split by whether the window contained a spike.** An overall
  MAE is dominated by the 98% of intervals that are quiet, so a model can look
  excellent while being useless exactly where money is made.
- **Brier, not accuracy.** Spikes are rare; a model that always says "no spike"
  scores 98% accuracy and is worth nothing.
- **Precision-recall AUC, not ROC AUC.** With a 1.8% positive rate ROC flatters
  everything, because the huge true-negative count swamps the denominator.
- **A reliability curve.** A probability that is not calibrated cannot drive the
  dispatch rule in 4.7: "70%" has to mean seven times in ten or the threshold
  is meaningless.

No numpy or scikit-learn. Every function here is a sum over a list, and adding
30 MB of wheels to compute a mean would be a poor trade — the `ml` extra waits
for the model that actually needs it.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Final

# PLAN 4.2: a spike is an interval above $300/MWh.
SPIKE_THRESHOLD: Final = 300.0


@dataclass(frozen=True)
class Regression:
    """Error of a mean-price forecast, overall and where it matters."""

    n: int
    mae: float
    rmse: float
    bias: float  # mean signed error; a model can be accurate and systematically low
    n_spike: int
    mae_spike: float | None
    mae_quiet: float | None

    def beats(self, other: Regression) -> bool:
        return self.mae < other.mae


@dataclass(frozen=True)
class Classification:
    """Quality of a spike probability."""

    n: int
    positives: int
    base_rate: float
    brier: float
    pr_auc: float
    # (predicted mean, observed rate, count) per bin — the reliability curve.
    reliability: list[tuple[float, float, int]]


def _pairs(predicted: list[float], actual: list[float]) -> list[tuple[float, float]]:
    if len(predicted) != len(actual):
        raise ValueError(f"length mismatch: {len(predicted)} predictions, {len(actual)} actuals")
    return [
        (p, a)
        for p, a in zip(predicted, actual, strict=True)
        if p is not None and a is not None
    ]


def regression(predicted: list[float], actual: list[float]) -> Regression:
    """MAE, RMSE and bias, split by whether the actual window held a spike."""
    pairs = _pairs(predicted, actual)
    if not pairs:
        raise ValueError("no comparable pairs")

    errors = [p - a for p, a in pairs]
    n = len(errors)
    mae = sum(abs(e) for e in errors) / n
    rmse = math.sqrt(sum(e * e for e in errors) / n)
    bias = sum(errors) / n

    spike = [abs(p - a) for p, a in pairs if a > SPIKE_THRESHOLD]
    quiet = [abs(p - a) for p, a in pairs if a <= SPIKE_THRESHOLD]
    return Regression(
        n=n,
        mae=mae,
        rmse=rmse,
        bias=bias,
        n_spike=len(spike),
        mae_spike=sum(spike) / len(spike) if spike else None,
        mae_quiet=sum(quiet) / len(quiet) if quiet else None,
    )


def brier(predicted: list[float], actual: list[float]) -> float:
    """Mean squared error of a probability against a 0/1 outcome."""
    pairs = _pairs(predicted, actual)
    if not pairs:
        raise ValueError("no comparable pairs")
    return sum((p - a) ** 2 for p, a in pairs) / len(pairs)


def pr_auc(predicted: list[float], actual: list[float]) -> float:
    """Average precision: the area under the precision-recall curve.

    Computed as the step-wise sum that `sklearn.average_precision_score` uses —
    precision at each threshold weighted by the gain in recall — rather than a
    trapezoid, which is optimistic on a curve this jagged.

    Ties matter here. Several intervals often share a predicted probability
    (every baseline that outputs 0 or 1, for one), and ranking them arbitrarily
    would invent discrimination the model does not have, so tied scores are
    consumed as a block.
    """
    pairs = sorted(_pairs(predicted, actual), key=lambda pa: -pa[0])
    if not pairs:
        raise ValueError("no comparable pairs")
    total_positive = sum(1 for _, a in pairs if a > 0.5)
    if total_positive == 0:
        return 0.0

    score = 0.0
    seen = seen_positive = 0
    previous_recall = 0.0
    i = 0
    while i < len(pairs):
        threshold = pairs[i][0]
        while i < len(pairs) and pairs[i][0] == threshold:
            seen += 1
            seen_positive += 1 if pairs[i][1] > 0.5 else 0
            i += 1
        precision = seen_positive / seen
        recall = seen_positive / total_positive
        score += precision * (recall - previous_recall)
        previous_recall = recall
    return score


def reliability(
    predicted: list[float], actual: list[float], bins: int = 10
) -> list[tuple[float, float, int]]:
    """Predicted probability against observed frequency, in equal-width bins.

    Empty bins are omitted rather than reported as zero: "no predictions in this
    range" and "predictions here were always wrong" are different facts.
    """
    pairs = _pairs(predicted, actual)
    buckets: dict[int, list[tuple[float, float]]] = {}
    for p, a in pairs:
        index = min(int(p * bins), bins - 1)
        buckets.setdefault(index, []).append((p, a))

    out = []
    for index in sorted(buckets):
        group = buckets[index]
        mean_predicted = sum(p for p, _ in group) / len(group)
        observed = sum(1 for _, a in group if a > 0.5) / len(group)
        out.append((mean_predicted, observed, len(group)))
    return out


def classification(predicted: list[float], actual: list[float], bins: int = 10) -> Classification:
    """Every spike metric at once."""
    pairs = _pairs(predicted, actual)
    if not pairs:
        raise ValueError("no comparable pairs")
    positives = sum(1 for _, a in pairs if a > 0.5)
    return Classification(
        n=len(pairs),
        positives=positives,
        base_rate=positives / len(pairs),
        brier=brier(predicted, actual),
        pr_auc=pr_auc(predicted, actual),
        reliability=reliability(predicted, actual, bins),
    )
