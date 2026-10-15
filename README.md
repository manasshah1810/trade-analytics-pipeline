# trade-analytics-pipeline

[![CI](https://github.com/manasshah1810/trade-analytics-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/manasshah1810/trade-analytics-pipeline/actions/workflows/ci.yml)

A PySpark batch pipeline that turns raw transaction events (the same JSON format used by
[recon-engine](https://github.com/manasshah1810/recon-engine)'s Kafka ingestion) into clean, reconciled,
analytics-ready Parquet tables, and explains every break.

## What it does

```
raw JSON lines -> parse + validate -> dedupe -> exact 1:1 reconcile -> daily break summary (Spark SQL)
                       |                                 |            \-> break aging buckets
                       |                                 \-> classify each break by cause -> break types (Spark SQL)
                       \-> quarantine (bad records + reason) -> quarantine summary
```

1. **Parse/validate** against an explicit all-string schema, then cast. Invalid rows (missing field, side not A/B, non-numeric
   amount, non-ISO date, bad currency) go to `quarantine/` with a `reason`; nothing is silently dropped.
2. **Dedupe** on `(streamId, eventId)` because delivery is at-least-once.
3. **Reconcile** side A to side B one-to-one on stream, reference, currency, amount and date (window `row_number` + join,
   so duplicates pair in event-id order).
4. **Classify breaks:** leftover A and B lines are paired on reference and labelled `MISSING_A`, `MISSING_B`,
   `CURRENCY_MISMATCH`, `DATE_MISMATCH`, `AMOUNT_MISMATCH` or `WITHIN_TOLERANCE` (amount difference at most `--tolerance`,
   default 0.05, reported but not treated as a match), each with an exposure amount.
5. **Analytics (Spark SQL):** per date and currency matched pairs, unmatched lines and value, break rate; breaks and
   exposure per type and currency; aging buckets (0-1d, 2-7d, 8-30d, 30d+) relative to `--as-of`; rejected records per reason.
6. Output: `breaks/`, `break_report/` and `matches/` (Parquet partitioned by `trade_date`), `daily_summary/`, `break_types/`,
   `aging/`, `quarantine_summary/`, `quarantine/`. Overwrite mode, so reruns on the same input are idempotent.

## Run

```bash
pip install -r requirements.txt            # needs JDK 17+
python scripts/generate_events.py --out data/raw --pairs 100000 --seed 7
python -m pipeline.job --input data/raw --output data/out --as-of 2026-03-31 --tolerance 0.05
```

The job prints one JSON line of counts, including the number of breaks of each type.

## Tests

```bash
pytest -q
```

30 tests on a local SparkSession: validation and quarantine reasons, dedupe, reconcile edge cases (currency/date mismatch,
duplicates, trailing zeros, cross-stream, empty input), a regression test for event ids repeated across streams, break
classification (each type, inclusive tolerance boundary, configurable tolerance, precedence, duplicate overflow, one row per
break), summary maths and aging buckets.

CI also runs the whole job on 20,000 generated pairs and checks the job's counts against the generator's exact defect
counts: matched pairs, breaks, quarantined, deduped, and the number of breaks of each type (570 missing, 303 amount
mismatches, 313 within tolerance), including the Parquet writes on Linux.

## Limitations

- Exact matching on the first pass; the second pass only labels leftovers and does not turn them into matches.
- Local mode only; not run on a cluster or on Snowflake. Synthetic data. No performance claims are made.
- On Windows, Parquet writes need winutils; tests do not write files, and the end-to-end run is verified on Linux in CI.
