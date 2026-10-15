from datetime import date
from decimal import Decimal

import pytest
from pyspark.sql import SparkSession

from pipeline import transforms as T


@pytest.fixture(scope="session")
def spark():
    s = (SparkSession.builder.master("local[2]").appName("tests")
         .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "2").getOrCreate())
    yield s
    s.stop()


def ev(stream="s", eid="e1", side="A", ref="T1", amount="10.00", ccy="USD", d="2026-03-01"):
    return (stream, eid, side, ref, amount, ccy, d)


def raw(spark, rows):
    return spark.createDataFrame(rows, T.RAW_SCHEMA)


def valid(spark, rows):
    return T.parse_events(raw(spark, rows))[0]


# ---- parse_events -------------------------------------------------------------------------------------------

def test_valid_row_is_typed_and_normalised(spark):
    v, q = T.parse_events(raw(spark, [ev(side=" a ", ref=" t1 ", ccy="usd")]))
    row = v.collect()[0]
    assert (row.side, row.reference, row.currency) == ("A", "T1", "USD")
    assert row.amount == Decimal("10.0000") and row.trade_date == date(2026, 3, 1)
    assert q.count() == 0


@pytest.mark.parametrize("bad,reason", [
    (ev(side="C"), "bad side"),
    (ev(amount="abc"), "bad amount"),
    (ev(d="01/03/2026"), "bad date"),
    (ev(ccy="US"), "bad currency"),
    (ev(ref=None), "missing field"),
    (ev(eid=None), "missing field"),
])
def test_invalid_rows_are_quarantined_with_reason(spark, bad, reason):
    v, q = T.parse_events(raw(spark, [bad]))
    assert v.count() == 0
    assert [r.reason for r in q.collect()] == [reason]


def test_good_and_bad_rows_are_split_without_loss(spark):
    rows = [ev(eid="1"), ev(eid="2", amount="x"), ev(eid="3")]
    v, q = T.parse_events(raw(spark, rows))
    assert v.count() == 2 and q.count() == 1


# ---- dedupe -------------------------------------------------------------------------------------------------

def test_dedupe_keeps_one_row_per_event_id(spark):
    df = valid(spark, [ev(eid="1"), ev(eid="1"), ev(eid="2")])
    assert T.dedupe(df).count() == 2


def test_same_event_id_in_different_streams_is_kept(spark):
    df = valid(spark, [ev(stream="s1", eid="1"), ev(stream="s2", eid="1")])
    assert T.dedupe(df).count() == 2


# ---- exact_reconcile ----------------------------------------------------------------------------------------

def test_exact_pair_matches(spark):
    df = valid(spark, [ev(eid="a1", side="A"), ev(eid="b1", side="B")])
    matched, unmatched = T.exact_reconcile(df)
    assert matched.count() == 1 and unmatched.count() == 0


def test_amount_difference_leaves_two_breaks(spark):
    df = valid(spark, [ev(eid="a1", side="A", amount="10.00"), ev(eid="b1", side="B", amount="10.01")])
    matched, unmatched = T.exact_reconcile(df)
    assert matched.count() == 0
    assert sorted(r.side for r in unmatched.collect()) == ["A", "B"]


def test_currency_and_date_must_agree(spark):
    df = valid(spark, [ev(eid="a1", side="A"), ev(eid="b1", side="B", ccy="EUR"),
                       ev(eid="a2", side="A", ref="T2"), ev(eid="b2", side="B", ref="T2", d="2026-03-02")])
    matched, unmatched = T.exact_reconcile(df)
    assert matched.count() == 0 and unmatched.count() == 4


def test_duplicates_pair_one_to_one(spark):
    rows = [ev(eid=f"a{i}", side="A") for i in range(3)] + [ev(eid=f"b{i}", side="B") for i in range(2)]
    matched, unmatched = T.exact_reconcile(valid(spark, rows))
    assert matched.count() == 2
    assert [r.eventId for r in unmatched.collect()] == ["a2"]


def test_trailing_zeros_in_amount_still_match(spark):
    df = valid(spark, [ev(eid="a1", side="A", amount="10.0"), ev(eid="b1", side="B", amount="10.00")])
    assert T.exact_reconcile(df)[0].count() == 1


def test_different_streams_do_not_cross_match(spark):
    df = valid(spark, [ev(stream="s1", eid="a1", side="A"), ev(stream="s2", eid="b1", side="B")])
    assert T.exact_reconcile(df)[0].count() == 0


def test_empty_input(spark):
    df = valid(spark, [])
    matched, unmatched = T.exact_reconcile(df)
    assert matched.count() == 0 and unmatched.count() == 0


# ---- analytics ----------------------------------------------------------------------------------------------

def test_daily_break_summary_counts_and_rate(spark):
    rows = [ev(eid="a1", side="A", ref="T1"), ev(eid="b1", side="B", ref="T1"),     # 1 pair
            ev(eid="a2", side="A", ref="T2", amount="5.00"),                          # 1 break
            ev(eid="a3", side="A", ref="T3", d="2026-03-02", ccy="EUR", amount="-7.00")]
    matched, unmatched = T.exact_reconcile(valid(spark, rows))
    out = {(r.trade_date, r.currency): r for r in T.daily_break_summary(spark, matched, unmatched).collect()}
    usd = out[(date(2026, 3, 1), "USD")]
    assert (usd.matched_pairs, usd.unmatched_lines, usd.unmatched_value) == (1, 1, Decimal("5.0000"))
    assert float(usd.break_rate) == pytest.approx(1 / 3, abs=1e-3)
    eur = out[(date(2026, 3, 2), "EUR")]
    assert eur.matched_pairs == 0 and float(eur.break_rate) == 1.0 and eur.unmatched_value == Decimal("7.0000")


def test_aging_buckets(spark):
    rows = [ev(eid="1", d="2026-03-31"), ev(eid="2", d="2026-03-29"), ev(eid="3", d="2026-03-10"), ev(eid="4", d="2026-01-01")]
    _, unmatched = T.exact_reconcile(valid(spark, rows))
    got = {r.bucket: r.lines for r in T.aging(unmatched, "2026-03-31").collect()}
    assert got == {"0-1d": 1, "2-7d": 1, "8-30d": 1, "30d+": 1}


# ---- regression: event ids are only unique within a stream -----------------------------------------------------

def test_same_event_id_in_another_stream_is_not_dropped_as_matched(spark):
    rows = [ev(stream="s1", eid="1", side="A"), ev(stream="s1", eid="2", side="B"),   # s1 pairs
            ev(stream="s2", eid="1", side="A", ref="T9")]                             # s2 shares eventId "1", has no pair
    matched, unmatched = T.exact_reconcile(valid(spark, rows))
    assert matched.count() == 1
    assert [(r.streamId, r.eventId) for r in unmatched.collect()] == [("s2", "1")]


# ---- classify_breaks ----------------------------------------------------------------------------------------

def classify(spark, rows, tol="0.05"):
    _, unmatched = T.exact_reconcile(valid(spark, rows))
    return {r.reference + "/" + r.break_type: r for r in T.classify_breaks(unmatched, tol).collect()}


def test_missing_side_is_classified_by_which_side_is_absent(spark):
    got = classify(spark, [ev(eid="a1", side="A", ref="T1"), ev(eid="b2", side="B", ref="T2")])
    assert set(got) == {"T1/MISSING_B", "T2/MISSING_A"}
    assert got["T1/MISSING_B"].exposure == Decimal("10.0000")


def test_amount_mismatch_reports_difference_as_exposure(spark):
    got = classify(spark, [ev(eid="a1", side="A", amount="100.00"), ev(eid="b1", side="B", amount="112.50")])
    assert list(got) == ["T1/AMOUNT_MISMATCH"]
    assert got["T1/AMOUNT_MISMATCH"].exposure == Decimal("12.5000")


def test_tolerance_boundary_is_inclusive(spark):
    on_limit = classify(spark, [ev(eid="a1", side="A", amount="10.00"), ev(eid="b1", side="B", amount="10.05")])
    over = classify(spark, [ev(eid="a1", side="A", amount="10.00"), ev(eid="b1", side="B", amount="10.06")])
    assert list(on_limit) == ["T1/WITHIN_TOLERANCE"] and list(over) == ["T1/AMOUNT_MISMATCH"]


def test_tolerance_is_configurable_and_symmetric(spark):
    rows = [ev(eid="a1", side="A", amount="10.00"), ev(eid="b1", side="B", amount="9.00")]
    assert list(classify(spark, rows, tol="1.00")) == ["T1/WITHIN_TOLERANCE"]
    assert list(classify(spark, rows, tol="0.99")) == ["T1/AMOUNT_MISMATCH"]


def test_currency_beats_date_beats_amount(spark):
    rows = [ev(eid="a1", side="A", ref="C", ccy="USD", d="2026-03-01", amount="1.00"),
            ev(eid="b1", side="B", ref="C", ccy="EUR", d="2026-03-02", amount="9.00"),
            ev(eid="a2", side="A", ref="D", d="2026-03-01", amount="1.00"),
            ev(eid="b2", side="B", ref="D", d="2026-03-02", amount="9.00")]
    got = classify(spark, rows)
    assert set(got) == {"C/CURRENCY_MISMATCH", "D/DATE_MISMATCH"}


def test_leftover_duplicates_pair_then_overflow_is_missing(spark):
    rows = [ev(eid="a1", side="A", amount="5.00"), ev(eid="a2", side="A", amount="6.00"),
            ev(eid="b1", side="B", amount="7.00")]
    types = sorted(r.break_type for r in T.classify_breaks(T.exact_reconcile(valid(spark, rows))[1]).collect())
    assert types == ["AMOUNT_MISMATCH", "MISSING_B"]


def test_one_row_per_break_and_no_double_counting(spark):
    rows = [ev(eid="a1", side="A", ref="T1", amount="1.00"), ev(eid="b1", side="B", ref="T1", amount="2.00"),
            ev(eid="a2", side="A", ref="T2"), ev(eid="b3", side="B", ref="T3")]
    _, unmatched = T.exact_reconcile(valid(spark, rows))
    assert unmatched.count() == 4
    assert T.classify_breaks(unmatched).count() == 3        # one mismatch (2 lines) + 2 singles


def test_no_breaks_gives_empty_report(spark):
    _, unmatched = T.exact_reconcile(valid(spark, [ev(eid="a1", side="A"), ev(eid="b1", side="B")]))
    assert T.classify_breaks(unmatched).count() == 0


def test_break_type_summary_sums_exposure_per_type_and_currency(spark):
    rows = [ev(eid="a1", side="A", ref="T1", amount="10.00"), ev(eid="a2", side="A", ref="T2", amount="4.00"),
            ev(eid="a3", side="A", ref="T3", amount="3.00", ccy="EUR"),
            ev(eid="a4", side="A", ref="T4", amount="1.00"), ev(eid="b4", side="B", ref="T4", amount="3.50")]
    _, unmatched = T.exact_reconcile(valid(spark, rows))
    out = T.break_type_summary(spark, T.classify_breaks(unmatched))
    got = {(r.break_type, r.currency): (r.breaks, r.exposure) for r in out.collect()}
    assert got == {("MISSING_B", "USD"): (2, Decimal("14.0000")), ("MISSING_B", "EUR"): (1, Decimal("3.0000")),
                   ("AMOUNT_MISMATCH", "USD"): (1, Decimal("2.5000"))}
    exposures = [r.exposure for r in out.collect()]
    assert exposures == sorted(exposures, reverse=True)


# ---- quarantine_summary ------------------------------------------------------------------------------------

def test_quarantine_summary_counts_reasons(spark):
    rows = [ev(eid="1", amount="x"), ev(eid="2", amount="y"), ev(eid="3", side="Z"), ev(eid="4")]
    _, q = T.parse_events(raw(spark, rows))
    got = [(r.reason, r.records) for r in T.quarantine_summary(q).collect()]
    assert got == [("bad amount", 2), ("bad side", 1)]
