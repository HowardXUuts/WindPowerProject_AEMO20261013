"""Tests for the forecast bronze sources: P5MIN and PredispatchIS.

The fixtures use AEMO's real column names and real sample values, taken from
the live files scripts/inspect_report.py downloaded on 2026-10-15. A fixture
with invented column names would pass against an invented spec and prove
nothing, which is the failure these sources were held back to avoid.

What these tests are really guarding is the leakage rule. A forecast row whose
issue time is missing, or silently swapped for the interval it forecasts, is a
model that looks superb and earns nothing.
"""

from datetime import datetime, timedelta

import pytest
from ingest_dispatchis import DISPATCHIS, INTERCONNECTOR_TABLE, REGIONSUM_TABLE
from ingest_p5min import P5MIN
from ingest_p5min import REGION_SOLUTION as P5_SOLUTION
from ingest_predispatch import PREDISPATCH, REGION_PRICES
from ingest_predispatch import REGION_SOLUTION as PD_SOLUTION
from nemweb import NEM_TZ, ParseError, extract_rows, interval_from_filename

P5_FILE = "PUBLIC_P5MIN_202610150950_20261015094603.zip"
PD_FILE = "PUBLIC_PREDISPATCHIS_202610151000_20261015093125.zip"
DIS_FILE = "PUBLIC_DISPATCHIS_202610150950_0000000123456789.zip"


def columns_of(spec):
    return {column.name: i for i, column in enumerate(spec.columns)}


# --- P5MIN ----------------------------------------------------------------
#
# Real header order, trimmed to the declared columns plus neighbours. 5 regions
# x 3 intervals; the real file carries 12 intervals.

P5_HEADER = (
    "I,P5MIN,REGIONSOLUTION,1,RUN_DATETIME,INTERVENTION,INTERVAL_DATETIME,REGIONID,"
    "RRP,TOTALDEMAND,LASTCHANGED,UIGF,SS_SOLAR_UIGF,SS_WIND_UIGF"
)


def p5_csv(intervals: int = 3, regions: tuple[str, ...] = ("NSW1", "SA1")) -> str:
    lines = ["C,NEMP,P5MIN,AEMO,PUBLIC,2026/10/15 09:46:03,1,,", P5_HEADER]
    for n in range(intervals):
        minute = 50 + n * 5
        hour, minute = (9 + minute // 60), minute % 60
        for region in regions:
            lines.append(
                f'D,P5MIN,REGIONSOLUTION,1,"2026/10/15 09:50:00",0,'
                f'"2026/10/15 {hour:02d}:{minute:02d}:00",{region},'
                f"{80.12 + n},{6338.75 + n},"
                f'"2026/10/15 09:45:59",4585.85,3508.36,1077.49'
            )
    return "\n".join(lines) + '\nC,"END OF REPORT",10\n'


def test_p5min_keeps_both_timestamps_and_they_differ():
    rows = extract_rows(P5_SOLUTION, p5_csv(), P5_FILE)
    at = columns_of(P5_SOLUTION)
    row = rows[0]
    # The run is named 09:50 but was written at 09:45:59. Collapsing the two
    # would hand the model a forecast four minutes before it existed, or hide
    # that it existed early. Both are kept.
    assert row[at["run_datetime"]] == datetime(2026, 10, 15, 9, 50, tzinfo=NEM_TZ)
    assert row[at["lastchanged"]] == datetime(2026, 10, 15, 9, 45, 59, tzinfo=NEM_TZ)
    assert row[at["lastchanged"]] < row[at["run_datetime"]]


def test_p5min_interval_is_distinct_from_the_run():
    rows = extract_rows(P5_SOLUTION, p5_csv(intervals=3), P5_FILE)
    at = columns_of(P5_SOLUTION)
    runs = {row[at["run_datetime"]] for row in rows}
    intervals = {row[at["interval_datetime"]] for row in rows}
    assert len(runs) == 1, "one run per file"
    assert len(intervals) == 3, "three horizons in this fixture"
    assert min(intervals) == datetime(2026, 10, 15, 9, 50, tzinfo=NEM_TZ)


def test_p5min_carries_the_residual_the_plan_asks_for():
    rows = extract_rows(P5_SOLUTION, p5_csv(regions=("NSW1",)), P5_FILE)
    at = columns_of(P5_SOLUTION)
    row = rows[0]
    assert row[at["rrp"]] == pytest.approx(80.12)
    assert row[at["totaldemand"]] == pytest.approx(6338.75)
    assert row[at["ss_wind_uigf"]] == pytest.approx(1077.49)
    assert row[at["ss_solar_uigf"]] == pytest.approx(3508.36)


def test_p5min_keeps_every_region_of_every_interval():
    rows = extract_rows(P5_SOLUTION, p5_csv(intervals=3, regions=("NSW1", "SA1")), P5_FILE)
    assert len(rows) == 6


def test_p5min_horizon_would_trim_a_longer_run():
    # AEMO publishes 12 today; the declared trim is what bounds the cost if
    # that ever grows, so it has to actually bite.
    rows = extract_rows(P5_SOLUTION, p5_csv(intervals=20, regions=("NSW1",)), P5_FILE)
    assert len(rows) == 12


def test_p5min_filename_parses_as_its_run_interval():
    assert interval_from_filename(P5MIN, P5_FILE) == datetime(
        2026, 10, 15, 9, 50, tzinfo=NEM_TZ
    )


# --- PredispatchIS --------------------------------------------------------
#
# The report with no RUN_DATETIME. Run identity is PREDISPATCHSEQNO and the
# issue time is LASTCHANGED.

PD_PRICE_HEADER = (
    "I,PREDISPATCH,REGION_PRICES,1,PREDISPATCHSEQNO,RUNNO,REGIONID,PERIODID,"
    "INTERVENTION,RRP,EEP,LASTCHANGED,DATETIME"
)
PD_SOLUTION_HEADER = (
    "I,PREDISPATCH,REGION_SOLUTION,1,PREDISPATCHSEQNO,RUNNO,REGIONID,PERIODID,"
    "INTERVENTION,TOTALDEMAND,AVAILABLEGENERATION,NETINTERCHANGE,LASTCHANGED,"
    "DATETIME,SS_SOLAR_UIGF,SS_WIND_UIGF"
)


PD_FIRST_PERIOD = datetime(2026, 10, 15, 10, 0)


def pd_stamp(n: int) -> str:
    """The nth half-hour of the run, rolling into the next day as AEMO's does."""
    return (PD_FIRST_PERIOD + timedelta(minutes=30 * n)).strftime("%Y/%m/%d %H:%M:%S")


def pd_csv(periods: int = 6, regions: tuple[str, ...] = ("QLD1", "SA1")) -> str:
    lines = ["C,NEMP,PREDISPATCH,AEMO,PUBLIC,2026/10/15 09:31:25,1,,", PD_PRICE_HEADER]
    for n in range(periods):
        for region in regions:
            lines.append(
                f'D,PREDISPATCH,REGION_PRICES,1,2026101512,1,{region},{n + 1:02d},0,'
                f'{64.57 + n},0,"2026/10/15 09:31:18","{pd_stamp(n)}"'
            )
    lines.append(PD_SOLUTION_HEADER)
    for n in range(periods):
        for region in regions:
            lines.append(
                f'D,PREDISPATCH,REGION_SOLUTION,1,2026101512,1,{region},{n + 1:02d},0,'
                f'{5462.09 + n},11887.478,342.6,"2026/10/15 09:31:18",'
                f'"{pd_stamp(n)}",1612.204,1412.274'
            )
    return "\n".join(lines) + '\nC,"END OF REPORT",20\n'


def test_predispatch_issue_time_comes_from_lastchanged():
    rows = extract_rows(REGION_PRICES, pd_csv(), PD_FILE)
    at = columns_of(REGION_PRICES)
    row = rows[0]
    # This report has no RUN_DATETIME at all. LASTCHANGED is the only issue
    # time it carries, and the leakage rule stands on it.
    assert row[at["lastchanged"]] == datetime(2026, 10, 15, 9, 31, 18, tzinfo=NEM_TZ)
    assert row[at["predispatchseqno"]] == "2026101512"


def test_predispatch_is_issued_well_before_what_it_forecasts():
    rows = extract_rows(REGION_PRICES, pd_csv(), PD_FILE)
    at = columns_of(REGION_PRICES)
    for row in rows:
        assert row[at["lastchanged"]] < row[at["datetime"]], (
            "a forecast issued after the period it forecasts is leakage"
        )


def test_predispatch_keeps_only_the_nearest_four_half_hours():
    rows = extract_rows(REGION_PRICES, pd_csv(periods=37, regions=("QLD1",)), PD_FILE)
    at = columns_of(REGION_PRICES)
    kept = sorted(row[at["datetime"]].strftime("%H:%M") for row in rows)
    assert kept == ["10:00", "10:30", "11:00", "11:30"]


def test_predispatch_trims_both_tables_the_same_way():
    prices = extract_rows(REGION_PRICES, pd_csv(periods=37), PD_FILE)
    solution = extract_rows(PD_SOLUTION, pd_csv(periods=37), PD_FILE)
    # 4 periods x 2 regions, on both tables. A price without its solution, or
    # the reverse, would not join.
    assert len(prices) == len(solution) == 8


def test_predispatch_solution_carries_the_residual():
    rows = extract_rows(PD_SOLUTION, pd_csv(regions=("QLD1",)), PD_FILE)
    at = columns_of(PD_SOLUTION)
    row = rows[0]
    assert row[at["totaldemand"]] == pytest.approx(5462.09)
    assert row[at["availablegeneration"]] == pytest.approx(11887.478)
    assert row[at["netinterchange"]] == pytest.approx(342.6)
    assert row[at["ss_wind_uigf"]] == pytest.approx(1412.274)


def test_predispatch_periodid_stays_text():
    rows = extract_rows(REGION_PRICES, pd_csv(regions=("QLD1",)), PD_FILE)
    at = columns_of(REGION_PRICES)
    # AEMO zero-pads it. Parsing it as an integer would quietly rewrite the
    # source value, which bronze is not allowed to do.
    assert rows[0][at["periodid"]] == "01"


def test_predispatch_two_tables_come_from_one_file():
    text = pd_csv()
    assert extract_rows(REGION_PRICES, text, PD_FILE)
    assert extract_rows(PD_SOLUTION, text, PD_FILE)
    assert PREDISPATCH.keys == ("predispatch_price", "predispatch_region")


def test_predispatch_filename_parses():
    assert interval_from_filename(PREDISPATCH, PD_FILE) == datetime(
        2026, 10, 15, 10, 0, tzinfo=NEM_TZ
    )


# --- the DispatchIS pair, against the real headers -------------------------

REGIONSUM_HEADER = (
    "I,DISPATCH,REGIONSUM,1,SETTLEMENTDATE,RUNNO,REGIONID,DISPATCHINTERVAL,INTERVENTION,"
    "TOTALDEMAND,AVAILABLEGENERATION,NETINTERCHANGE,UIGF,SS_SOLAR_UIGF,SS_WIND_UIGF,"
    "BDU_ENERGY_STORAGE,BDU_CLEAREDMW_GEN,BDU_CLEAREDMW_LOAD"
)
REGIONSUM_ROW = (
    'D,DISPATCH,REGIONSUM,1,"2026/10/15 09:50:00",1,NSW1,20261015070,0,'
    "6338.75,14671.58517,-656.12,4585.85791,3508.36625,1077.49166,"
    "2911.093790,0,527.992520"
)

INTERCONNECTOR_HEADER = (
    "I,DISPATCH,INTERCONNECTORRES,1,SETTLEMENTDATE,RUNNO,INTERCONNECTORID,"
    "DISPATCHINTERVAL,INTERVENTION,METEREDMWFLOW,MWFLOW,MWLOSSES,LASTCHANGED,"
    "EXPORTLIMIT,IMPORTLIMIT"
)
INTERCONNECTOR_ROW = (
    'D,DISPATCH,INTERCONNECTORRES,1,"2026/10/15 09:50:00",1,N-Q-MNSP1,20261015070,0,'
    '-16.7998,-25,1.04063,"2026/10/15 09:45:09",50.55134,-25.00021'
)

DISPATCHIS_CSV = "\n".join(
    [REGIONSUM_HEADER, REGIONSUM_ROW, INTERCONNECTOR_HEADER, INTERCONNECTOR_ROW]
) + '\nC,"END OF REPORT",4\n'


def test_regionsum_parses_the_real_header():
    rows = extract_rows(REGIONSUM_TABLE, DISPATCHIS_CSV, DIS_FILE)
    at = columns_of(REGIONSUM_TABLE)
    row = rows[0]
    assert row[at["totaldemand"]] == pytest.approx(6338.75)
    assert row[at["availablegeneration"]] == pytest.approx(14671.58517)
    assert row[at["netinterchange"]] == pytest.approx(-656.12)
    assert row[at["ss_wind_uigf"]] == pytest.approx(1077.49166)
    assert row[at["ss_solar_uigf"]] == pytest.approx(3508.36625)


def test_regionsum_carries_battery_storage():
    # PLAN 4.3 assumed state of charge had to be proxied because "AEMO does not
    # publish [it] directly". The inspection found BDU_ENERGY_STORAGE, so the
    # real quantity is available and the proxy is not needed.
    rows = extract_rows(REGIONSUM_TABLE, DISPATCHIS_CSV, DIS_FILE)
    at = columns_of(REGIONSUM_TABLE)
    assert rows[0][at["bdu_energy_storage"]] == pytest.approx(2911.09379)
    assert rows[0][at["bdu_clearedmw_load"]] == pytest.approx(527.99252)


def test_interconnector_keeps_flow_and_both_limits():
    rows = extract_rows(INTERCONNECTOR_TABLE, DISPATCHIS_CSV, DIS_FILE)
    at = columns_of(INTERCONNECTOR_TABLE)
    row = rows[0]
    assert row[at["interconnectorid"]] == "N-Q-MNSP1"
    assert row[at["mwflow"]] == pytest.approx(-25)
    # Flow alone cannot say a limit was binding; here it is, at the import end.
    assert row[at["importlimit"]] == pytest.approx(-25.00021)
    assert row[at["exportlimit"]] == pytest.approx(50.55134)


def test_the_three_dispatchis_tables_are_declared_together():
    assert DISPATCHIS.keys == ("price", "regionsum", "interconnector")


def test_a_renamed_aemo_column_names_what_arrived():
    broken = DISPATCHIS_CSV.replace("TOTALDEMAND", "TOTAL_DEMAND")
    with pytest.raises(ParseError) as exc:
        extract_rows(REGIONSUM_TABLE, broken, DIS_FILE)
    assert "TOTALDEMAND" in str(exc.value)
    assert "TOTAL_DEMAND" in str(exc.value), "the real header has to be in the message"
