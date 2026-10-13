"""Ingest AEMO Dispatch SCADA (generation) into the bronze layer.

PUBLIC_DISPATCHSCADA_*.zip carries the metered output in MW of every
dispatchable unit in the NEM, one file per five-minute interval.

Usage:
    python scripts/ingest_scada.py                 # ingest everything pending
    python scripts/ingest_scada.py --limit 12      # ingest at most 12 files
    python scripts/ingest_scada.py --list-only     # show what is pending
"""

from __future__ import annotations

import sys
from typing import Final

from cli import main_for
from nemweb import Column, ReportSpec, as_float, as_settlement_date, as_text

SCADA: Final = ReportSpec(
    key="scada",
    url="https://nemweb.com.au/Reports/Current/Dispatch_SCADA/",
    file_prefix="PUBLIC_DISPATCHSCADA",
    table="DISPATCH_UNIT_SCADA",
    columns=(
        Column("settlementdate", "TIMESTAMPTZ", "SETTLEMENTDATE", as_settlement_date),
        Column("duid", "VARCHAR", "DUID", as_text),
        Column("scadavalue", "DOUBLE", "SCADAVALUE", as_float),
    ),
)

main = main_for(SCADA, __doc__)

if __name__ == "__main__":
    sys.exit(main())
