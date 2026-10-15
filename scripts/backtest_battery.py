"""Battery arbitrage: what a forecast is actually worth, in dollars.

PLAN 4.7. A forecast is only worth what a decision made from it earns, so this
is the deciding metric — not MAE, which no one is paid for.

Four strategies run over the same price path, and the comparison is the point:

| Strategy          | What it establishes                                |
|-------------------|----------------------------------------------------|
| Perfect foresight | the ceiling — every dollar a perfect forecast could earn |
| Forecast-driven   | what this model is worth                            |
| Fixed threshold   | how much of that is the forecast rather than the rule |
| Do nothing        | zero                                                |

The third comparator is the one that keeps everyone honest. A forecast-driven
policy that earns $400/MW/day looks impressive until a rule with no forecast at
all earns $380.

**Settlement is always at the actual price**, never the forecast. A backtest
that settles at its own prediction is a model marking its own homework.

**Price-taking is assumed and it is an assumption.** A real battery of any size
moves the price it trades into, especially in the thin intervals where
arbitrage pays best. Nothing here models that, so these figures overstate what
a large unit would earn — which is why the headline number is the share of the
perfect-foresight ceiling rather than the dollars.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Final

LOG: Final = logging.getLogger("backtest")

MINUTES_PER_INTERVAL: Final = 5
HOURS_PER_INTERVAL: Final = MINUTES_PER_INTERVAL / 60.0


@dataclass(frozen=True)
class Battery:
    """A price-taking storage unit.

    Defaults are a plausible mid-size SA1 unit: 100 MW / 200 MWh is two hours of
    storage, the shape most Australian batteries built for arbitrage take.
    """

    power_mw: float = 100.0
    energy_mwh: float = 200.0
    round_trip_efficiency: float = 0.85
    soc_min: float = 0.05
    soc_max: float = 0.95
    initial_soc: float = 0.5

    @property
    def charge_efficiency(self) -> float:
        # Losses are taken on the charge leg, so a round trip loses exactly
        # round_trip_efficiency once rather than its square root twice.
        return self.round_trip_efficiency

    def clamp(self, soc_mwh: float) -> float:
        return max(self.energy_mwh * self.soc_min, min(self.energy_mwh * self.soc_max, soc_mwh))


@dataclass(frozen=True)
class Trade:
    """One interval's dispatch."""

    at: datetime
    mw: float  # positive discharging, negative charging
    rrp: float
    revenue_aud: float
    soc_mwh: float


@dataclass(frozen=True)
class Outcome:
    """What a strategy earned over the period."""

    name: str
    revenue_aud: float
    intervals: int
    cycles: float
    trades: list[Trade]

    def per_mw_day(self, battery: Battery) -> float:
        days = self.intervals * MINUTES_PER_INTERVAL / 1440
        if days <= 0 or battery.power_mw <= 0:
            return 0.0
        return self.revenue_aud / battery.power_mw / days


def _settle(
    battery: Battery,
    prices: list[tuple[datetime, float]],
    decide: Callable[[int, float, float], float],
    name: str,
) -> Outcome:
    """Run one strategy. ``decide(index, rrp, soc_mwh) -> requested MW``.

    Positive is discharge, negative is charge. The request is clipped to power
    and to the energy actually available, so a strategy cannot cheat by asking
    for more than the unit holds.
    """
    soc = battery.energy_mwh * battery.initial_soc
    revenue = 0.0
    charged = 0.0
    trades: list[Trade] = []

    for index, (at, rrp) in enumerate(prices):
        want = decide(index, rrp, soc)
        want = max(-battery.power_mw, min(battery.power_mw, want))

        if want > 0:  # discharging: energy leaves storage, cash comes in
            room = max(0.0, soc - battery.energy_mwh * battery.soc_min)
            mwh = min(want * HOURS_PER_INTERVAL, room)
            soc = battery.clamp(soc - mwh)
            revenue += mwh * rrp
            mw = mwh / HOURS_PER_INTERVAL
        elif want < 0:  # charging: cash goes out, less than that arrives as energy
            room = max(0.0, battery.energy_mwh * battery.soc_max - soc)
            drawn = min(-want * HOURS_PER_INTERVAL, room / battery.charge_efficiency)
            soc = battery.clamp(soc + drawn * battery.charge_efficiency)
            revenue -= drawn * rrp
            charged += drawn * battery.charge_efficiency
            mw = -drawn / HOURS_PER_INTERVAL
        else:
            mw = 0.0

        if mw != 0.0:
            trades.append(Trade(at, mw, rrp, revenue, soc))

    cycles = charged / battery.energy_mwh if battery.energy_mwh else 0.0
    return Outcome(name, revenue, len(prices), cycles, trades)


def do_nothing(battery: Battery, prices: list[tuple[datetime, float]]) -> Outcome:
    """The floor. A strategy that loses money has not beaten it."""
    return _settle(battery, prices, lambda i, p, s: 0.0, "do_nothing")


def fixed_threshold(
    battery: Battery,
    prices: list[tuple[datetime, float]],
    charge_below: float,
    discharge_above: float,
) -> Outcome:
    """Trade on the current price alone — no forecast anywhere.

    The comparator that says how much of a forecast-driven result is the
    forecast and how much is simply "buy cheap, sell dear".
    """

    def decide(index: int, rrp: float, soc: float) -> float:
        if rrp <= charge_below:
            return -battery.power_mw
        if rrp >= discharge_above:
            return battery.power_mw
        return 0.0

    return _settle(battery, prices, decide, "fixed_threshold")


def forecast_policy(
    battery: Battery,
    prices: list[tuple[datetime, float]],
    forecast_mean: list[float],
    forecast_spike: list[float] | None,
    charge_below: float,
    discharge_above: float,
    spike_threshold: float = 0.5,
) -> Outcome:
    """Trade on the forecast, settle on the actual price.

    Thresholds are arguments rather than constants because PLAN 4.7 requires
    them tuned inside the training fold. Tuning them on the test period is
    leakage wearing a different hat, and it is the easiest way to produce a
    backtest that cannot be repeated.
    """
    if len(forecast_mean) != len(prices):
        raise ValueError("forecast and price series differ in length")

    def decide(index: int, rrp: float, soc: float) -> float:
        expected = forecast_mean[index]
        if expected is None:
            return 0.0
        if forecast_spike is not None:
            spike = forecast_spike[index]
            if spike is not None and spike >= spike_threshold:
                return battery.power_mw
        if expected <= charge_below:
            return -battery.power_mw
        if expected >= discharge_above:
            return battery.power_mw
        return 0.0

    return _settle(battery, prices, decide, "forecast_policy")


def perfect_foresight(battery: Battery, prices: list[tuple[datetime, float]]) -> Outcome:
    """The ceiling: the best any forecast could do on this price path.

    A greedy pairing of the cheapest remaining interval with the dearest later
    one, repeated while a pair still profits after round-trip losses. Greedy is
    not provably optimal, so this is a tight lower bound on the true ceiling
    rather than the ceiling itself — stated plainly, because a capture ratio
    quoted against an understated ceiling flatters the model.

    Both limits bind, and the energy one is the easy one to forget. A pair
    buying at i and selling at j holds its energy across every interval between
    them, so the unit must have room for it the whole way — not merely power to
    move it at each end. Without that check a 10 MWh battery happily cycles
    60 MWh through a flat low-price block and the ceiling comes out six times
    too high.
    """
    per_interval = battery.power_mw * HOURS_PER_INTERVAL
    usable = battery.energy_mwh * (battery.soc_max - battery.soc_min)
    remaining = [per_interval] * len(prices)
    # held[k] is energy already committed to sit in storage during interval k.
    held = [0.0] * len(prices)
    revenue = 0.0
    charged = 0.0
    trades: list[Trade] = []

    while True:
        buy = sell = None
        best = 0.0
        best_mwh = 0.0
        for i, (_, low) in enumerate(prices):
            if remaining[i] <= 0:
                continue
            for j in range(i + 1, len(prices)):
                if remaining[j] <= 0:
                    continue
                margin = prices[j][1] * battery.charge_efficiency - low
                if margin <= best:
                    continue
                headroom = usable - max(held[i + 1 : j + 1], default=0.0)
                mwh = min(remaining[i], remaining[j], headroom)
                if mwh <= 1e-12:
                    continue
                best, buy, sell, best_mwh = margin, i, j, mwh
        if buy is None or best <= 0:
            break

        mwh = best_mwh
        remaining[buy] -= mwh
        remaining[sell] -= mwh
        for k in range(buy + 1, sell + 1):
            held[k] += mwh
        revenue += mwh * battery.charge_efficiency * prices[sell][1] - mwh * prices[buy][1]
        charged += mwh * battery.charge_efficiency
        rate = mwh / HOURS_PER_INTERVAL
        trades.append(Trade(prices[buy][0], -rate, prices[buy][1], revenue, 0.0))
        trades.append(Trade(prices[sell][0], rate, prices[sell][1], revenue, 0.0))

    cycles = charged / battery.energy_mwh if battery.energy_mwh else 0.0
    return Outcome("perfect_foresight", revenue, len(prices), cycles, trades)


def capture_ratio(outcome: Outcome, ceiling: Outcome) -> float | None:
    """Share of the perfect-foresight ceiling a strategy captured.

    The headline number from PLAN 4.7: "captured 34% of the theoretical
    maximum" survives a change of test period, where a dollar figure does not.
    None when the ceiling earned nothing — there was no arbitrage to capture,
    and a ratio against zero would be an artefact.
    """
    if ceiling.revenue_aud <= 0:
        return None
    return outcome.revenue_aud / ceiling.revenue_aud
