"""Ingest AEMO's half-hourly pre-dispatch forecast (PredispatchIS).

PUBLIC_PREDISPATCHIS_*.zip is published every half hour and forecasts out to
about 40 hours — 37 half-hour periods in the file inspected. Only the nearest
four are kept. The battery arbitrage backtest decides within the hour; the rest
would cost nine times the rows to answer a question nothing asks.

Two tables are taken, from one download: the regional prices and the regional
solution behind them.

Column names here were read off a live file by scripts/inspect_report.py on
2026-10-15, not recalled.

**This report has no RUN_DATETIME.** Dispatch has SETTLEMENTDATE and P5MIN has
RUN_DATETIME, but PredispatchIS identifies a run by PREDISPATCHSEQNO — a
date-plus-sequence string like "2026101512", not a timestamp — and records when
it was written in LASTCHANGED. In the inspected file PREDISPATCHSEQNO
2026101512 was written at 09:31:18 and forecasts from 10:00 onward, and the
file name's own second stamp (093125) agrees with LASTCHANGED.

That matters more here than anywhere else in the pipeline. The leakage rule
gates a feature on when the forecast was *issued*; the only issue time this
report carries is LASTCHANGED, so it is kept on every row and the sequence
number is kept beside it as the run's identity.

Usage:
    python scripts/ingest_predispatch.py
    python scripts/ingest_predispatch.py --list-only
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

# The run's identity and when it was issued. Repeated on both tables because
# each is written as a standalone bronze table, and a forecast row without its
# issue time is a leakage incident waiting to happen.
RUN_COLUMNS: Final = (
    Column("predispatchseqno", "VARCHAR", "PREDISPATCHSEQNO", as_text),
    Column("lastchanged", "TIMESTAMPTZ", "LASTCHANGED", as_settlement_date),
    Column("runno", "INTEGER", "RUNNO", as_int),
    # The half-hour being forecast, and its position in the run. DATETIME is
    # the join key; PERIODID stays text because AEMO zero-pads it ("01").
    Column("datetime", "TIMESTAMPTZ", "DATETIME", as_settlement_date),
    Column("periodid", "VARCHAR", "PERIODID", as_text),
    Column("regionid", "VARCHAR", "REGIONID", as_text),
    Column("intervention", "INTEGER", "INTERVENTION", as_int),
)

# Trim to the nearest four half-hours of each run. Grouped by PREDISPATCHSEQNO
# rather than by LASTCHANGED: the sequence number is the run's identity by
# definition, where a timestamp that varied by a second across rows would split
# one run into two and keep four periods of each.
NEAR_HORIZON: Final = Horizon(run_time="predispatchseqno", interval="datetime", periods=4)

REGION_PRICES: Final = TableSpec(
    key="predispatch_price",
    table="PREDISPATCH_REGION_PRICES",
    columns=(
        *RUN_COLUMNS,
        # The forecast price itself — the half-hourly baseline, and the signal a
        # battery would actually trade against beyond P5MIN's one-hour horizon.
        Column("rrp", "DOUBLE", "RRP", as_float),
    ),
    horizon=NEAR_HORIZON,
)

REGION_SOLUTION: Final = TableSpec(
    key="predispatch_region",
    table="PREDISPATCH_REGION_SOLUTION",
    columns=(
        *RUN_COLUMNS,
        # The same residual the dispatch and P5MIN tables carry, half an hour at
        # a time: demand, what is available to meet it, what the interconnector
        # will bring, and the variable generation that will displace it.
        Column("totaldemand", "DOUBLE", "TOTALDEMAND", as_float),
        Column("availablegeneration", "DOUBLE", "AVAILABLEGENERATION", as_float),
        Column("netinterchange", "DOUBLE", "NETINTERCHANGE", as_float),
        Column("ss_wind_uigf", "DOUBLE", "SS_WIND_UIGF", as_float),
        Column("ss_solar_uigf", "DOUBLE", "SS_SOLAR_UIGF", as_float),
    ),
    horizon=NEAR_HORIZON,
)

PREDISPATCH: Final = ReportSpec(
    url="https://nemweb.com.au/Reports/Current/PredispatchIS_Reports/",
    file_prefix="PUBLIC_PREDISPATCHIS",
    tables=(REGION_PRICES, REGION_SOLUTION),
)

main = main_for(PREDISPATCH, __doc__)

if __name__ == "__main__":
    sys.exit(main())
