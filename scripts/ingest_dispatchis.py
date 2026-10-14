"""Ingest the DispatchIS report: regional price, demand summary and interconnectors.

PUBLIC_DISPATCHIS_*.zip carries the whole dispatch case for each five-minute
interval. Three of its tables matter here, and they come out of one download:

- DISPATCH_PRICE       the regional reference price generators are settled at
- DISPATCH_REGIONSUM   regional demand, available generation, net interchange
- DISPATCH_INTERCONNECTORRES  flow and limits on each interconnector

Declaring them together is the point. The zip is about 40 KB and is fetched
once per five-minute interval; parsing three tables out of it costs nothing
extra, while a second script would double the download for no benefit. They
still keep separate watermarks and separate bronze directories, so one table
failing to parse never costs another its hour.

Two things about this report matter downstream:

- INTERVENTION. When AEMO intervenes in the market it publishes both a physical
  run and a pricing run for the same interval. Bronze keeps both, because
  discarding one is a business decision; silver picks the pricing run.
- The FCAS price columns are deliberately not ingested. They are eight separate
  ancillary service markets, irrelevant to wind energy revenue, and they would
  quadruple the column count. Adding them later means editing COLUMNS below,
  but history written before that change will not carry them.

Usage:
    python scripts/ingest_dispatchis.py                 # ingest everything pending
    python scripts/ingest_dispatchis.py --limit 12      # ingest at most 12 files
    python scripts/ingest_dispatchis.py --list-only     # show what is pending
"""

from __future__ import annotations

import sys
from typing import Final

from cli import main_for
from nemweb import Column, ReportSpec, TableSpec, as_float, as_int, as_settlement_date, as_text

# ---------------------------------------------------------------------------
# DISPATCH_PRICE — what a megawatt hour was worth, per region per interval
# ---------------------------------------------------------------------------

PRICE_TABLE: Final = TableSpec(
    key="price",
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

# ---------------------------------------------------------------------------
# DISPATCH_REGIONSUM — the supply-demand balance behind the price
#
# Every column here earns its place in the feature set (PLAN.md 4.3):
#
#   totaldemand          the demand side of the residual. Price is set by what
#                        the dispatchable stack has to cover, and that is
#                        demand minus the variable renewables below.
#   availablegeneration  how much capacity was offered. Demand against
#                        available generation is the reserve margin, which is
#                        what actually separates a $60 interval from a $300 one.
#   netinterchange       net flow out of the region. A region importing heavily
#                        is price-coupled to its neighbour; one at its limit is
#                        not. PLAN 4.3 lists it under "demand and system".
#   ss_wind_uigf         AEMO's own unconstrained intermittent generation
#   ss_solar_uigf        forecast for semi-scheduled wind and solar. These are
#                        the regional variable-renewable totals the plan calls
#                        for, published by AEMO rather than summed by us, and
#                        they are the other half of the residual.
#
# The two UIGF columns are optional: they were added to this table well after
# the report existed, and a file that predates them must not fail the run. An
# absent optional column is logged every time, so "it is always NULL" cannot
# pass unnoticed.
# ---------------------------------------------------------------------------

REGIONSUM_TABLE: Final = TableSpec(
    key="regionsum",
    table="DISPATCH_REGIONSUM",
    columns=(
        Column("settlementdate", "TIMESTAMPTZ", "SETTLEMENTDATE", as_settlement_date),
        Column("regionid", "VARCHAR", "REGIONID", as_text),
        Column("intervention", "INTEGER", "INTERVENTION", as_int),
        Column("totaldemand", "DOUBLE", "TOTALDEMAND", as_float),
        Column("availablegeneration", "DOUBLE", "AVAILABLEGENERATION", as_float),
        Column("netinterchange", "DOUBLE", "NETINTERCHANGE", as_float),
        Column("ss_wind_uigf", "DOUBLE", "SS_WIND_UIGF", as_float, optional=True),
        Column("ss_solar_uigf", "DOUBLE", "SS_SOLAR_UIGF", as_float, optional=True),
    ),
)

# ---------------------------------------------------------------------------
# DISPATCH_INTERCONNECTORRES — whether a neighbour's price can reach this region
#
#   mwflow       actual flow, signed by the interconnector's nominal direction
#   exportlimit  the binding constraint in each direction. Flow at its limit is
#   importlimit  the moment two regions decouple and their prices diverge, which
#                is exactly when a local forecast stops being able to borrow its
#                neighbour's. Flow alone cannot say whether a limit was binding.
# ---------------------------------------------------------------------------

INTERCONNECTOR_TABLE: Final = TableSpec(
    key="interconnector",
    table="DISPATCH_INTERCONNECTORRES",
    columns=(
        Column("settlementdate", "TIMESTAMPTZ", "SETTLEMENTDATE", as_settlement_date),
        Column("interconnectorid", "VARCHAR", "INTERCONNECTORID", as_text),
        Column("intervention", "INTEGER", "INTERVENTION", as_int),
        Column("mwflow", "DOUBLE", "MWFLOW", as_float),
        Column("exportlimit", "DOUBLE", "EXPORTLIMIT", as_float),
        Column("importlimit", "DOUBLE", "IMPORTLIMIT", as_float),
    ),
)

DISPATCHIS: Final = ReportSpec(
    url="https://nemweb.com.au/Reports/Current/DispatchIS_Reports/",
    file_prefix="PUBLIC_DISPATCHIS",
    tables=(PRICE_TABLE, REGIONSUM_TABLE, INTERCONNECTOR_TABLE),
)

main = main_for(DISPATCHIS, __doc__)

if __name__ == "__main__":
    sys.exit(main())
