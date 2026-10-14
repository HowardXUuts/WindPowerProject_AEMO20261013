"""Ingest AEMO's five-minute pre-dispatch forecast (P5MIN).

PUBLIC_P5MIN_*.zip is published every five minutes and forecasts the next
twelve dispatch intervals — an hour ahead, at the same grain the targets are
defined on. It is both the strongest feature in the plan and the baseline the
model has to beat (PLAN.md 4.3 and 4.6): a forecast that cannot beat AEMO's own
is a finding, not a failure to hide.

Only P5MIN_REGIONSOLUTION is kept. The file also carries the unit, constraint
and interconnector solutions — 14,541 constraint rows alone, against 60 region
rows — and none of them are in the feature set.

Column names here were read off a live file by scripts/inspect_report.py on
2026-10-15, not recalled.

**The two timestamps matter and are not interchangeable.** RUN_DATETIME is the
interval the run is named for; LASTCHANGED is when AEMO actually wrote it. In
the inspected file the run named 09:50 was written at 09:45:59, so it exists
before the interval it is named for. The leakage rule in CLAUDE.md gates a
feature on when the forecast was *issued*, so both are kept and LASTCHANGED is
the one to gate on.

Usage:
    python scripts/ingest_p5min.py
    python scripts/ingest_p5min.py --list-only
"""

from __future__ import annotations

import sys
from typing import Final

from cli import main_for
from nemweb import (
    Column,
    Horizon,
    ReportSpec,
    TableSpec,
    as_float,
    as_int,
    as_settlement_date,
    as_text,
)

REGION_SOLUTION: Final = TableSpec(
    key="p5min",
    table="P5MIN_REGIONSOLUTION",
    columns=(
        # When AEMO issued this run, twice over — see the module docstring.
        Column("run_datetime", "TIMESTAMPTZ", "RUN_DATETIME", as_settlement_date),
        Column("lastchanged", "TIMESTAMPTZ", "LASTCHANGED", as_settlement_date),
        # The interval being forecast. (run_datetime, interval_datetime) is the
        # grain the model joins on: one row per forecast, per horizon, per region.
        Column("interval_datetime", "TIMESTAMPTZ", "INTERVAL_DATETIME", as_settlement_date),
        Column("regionid", "VARCHAR", "REGIONID", as_text),
        # Kept for the same reason as in dispatch: an intervention run and a
        # pricing run can both exist, and choosing between them is silver's job.
        Column("intervention", "INTEGER", "INTERVENTION", as_int),
        # The baseline itself: AEMO's forecast of the price we are forecasting.
        Column("rrp", "DOUBLE", "RRP", as_float),
        # Forecast demand, and the forecast variable generation that will be
        # subtracted from it. The residual is what sets the price, and AEMO's
        # view of it an hour ahead is the single most informative feature
        # available at forecast time.
        Column("totaldemand", "DOUBLE", "TOTALDEMAND", as_float),
        Column("ss_wind_uigf", "DOUBLE", "SS_WIND_UIGF", as_float),
        # Solar is not modelled per unit anywhere in this project, but PLAN 4.3
        # calls for it as a regional total: it is half the daytime residual.
        Column("ss_solar_uigf", "DOUBLE", "SS_SOLAR_UIGF", as_float),
    ),
    # AEMO publishes exactly twelve intervals per run today. Declaring the trim
    # anyway states the intent and bounds the cost if that horizon ever grows.
    horizon=Horizon(run_time="run_datetime", interval="interval_datetime", periods=12),
)

P5MIN: Final = ReportSpec(
    url="https://nemweb.com.au/Reports/Current/P5_Reports/",
    file_prefix="PUBLIC_P5MIN",
    tables=(REGION_SOLUTION,),
)

main = main_for(P5MIN, __doc__)

if __name__ == "__main__":
    sys.exit(main())
