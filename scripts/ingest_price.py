"""Ingest AEMO dispatch regional spot price into the bronze layer.

PUBLIC_DISPATCHIS_*.zip carries the dispatch case for each five-minute
interval. The DISPATCH_PRICE table inside it holds the regional reference
price (RRP, $/MWh) that generators are settled against — the other half of the
revenue calculation, alongside the MW figures from Dispatch SCADA.

Two things about this report matter downstream:

- INTERVENTION. When AEMO intervenes in the market it publishes both a physical
  run and a pricing run for the same interval. Bronze keeps both, because
  discarding one is a business decision; silver picks the pricing run.
- The FCAS price columns are deliberately not ingested. They are eight separate
  ancillary service markets, irrelevant to wind energy revenue, and they would
  quadruple the column count. Adding them later means editing COLUMNS below,
  but history written before that change will not carry them.

Usage:
    python scripts/ingest_price.py                 # ingest everything pending
    python scripts/ingest_price.py --limit 12      # ingest at most 12 files
    python scripts/ingest_price.py --list-only     # show what is pending
"""

from __future__ import annotations

import sys
from typing import Final

from cli import main_for
from nemweb import Column, ReportSpec, as_float, as_int, as_settlement_date, as_text

PRICE: Final = ReportSpec(
    key="price",
    url="https://nemweb.com.au/Reports/Current/DispatchIS_Reports/",
    file_prefix="PUBLIC_DISPATCHIS",
    table="DISPATCH_PRICE",
    columns=(
        Column("settlementdate", "TIMESTAMPTZ", "SETTLEMENTDATE", as_settlement_date),
        Column("regionid", "VARCHAR", "REGIONID", as_text),
        Column("intervention", "INTEGER", "INTERVENTION", as_int),
        Column("runno", "INTEGER", "RUNNO", as_int),
        Column("dispatchinterval", "VARCHAR", "DISPATCHINTERVAL", as_text),
        Column("rrp", "DOUBLE", "RRP", as_float),
        Column("eep", "DOUBLE", "EEP", as_float),
        Column("rop", "DOUBLE", "ROP", as_float),
        Column("apcflag", "INTEGER", "APCFLAG", as_int),
        Column("marketsuspendedflag", "INTEGER", "MARKETSUSPENDEDFLAG", as_int),
    ),
)

main = main_for(PRICE, __doc__)

if __name__ == "__main__":
    sys.exit(main())
