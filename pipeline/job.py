"""Batch job: raw JSON-lines events -> clean, deduped, reconciled Parquet tables.

    python -m pipeline.job --input data/raw --output data/out --as-of 2026-03-31

Re-running with the same input overwrites the same output, so reruns are idempotent.
"""
import argparse
import json

from pyspark.sql import SparkSession

from pipeline import transforms as T


def build_spark(app: str = "trade-analytics") -> SparkSession:
    return (SparkSession.builder.appName(app)
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
            .getOrCreate())


def run(spark: SparkSession, input_path: str, output_path: str, as_of: str) -> dict:
    raw = spark.read.schema(T.RAW_SCHEMA).option("mode", "PERMISSIVE").json(input_path)
    valid, quarantine = T.parse_events(raw)
    events = T.dedupe(valid).cache()
    matched, unmatched = T.exact_reconcile(events)
    matched, unmatched = matched.cache(), unmatched.cache()
    summary = T.daily_break_summary(spark, matched, unmatched)
    aged = T.aging(unmatched, as_of)

    quarantine.write.mode("overwrite").json(f"{output_path}/quarantine")
    unmatched.write.mode("overwrite").partitionBy("trade_date").parquet(f"{output_path}/breaks")
    matched.write.mode("overwrite").partitionBy("trade_date").parquet(f"{output_path}/matches")
    summary.write.mode("overwrite").parquet(f"{output_path}/daily_summary")
    aged.write.mode("overwrite").parquet(f"{output_path}/aging")

    return {"raw": raw.count(), "valid": valid.count(), "after_dedupe": events.count(),
            "quarantined": quarantine.count(), "matched_pairs": matched.count(), "breaks": unmatched.count()}


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--as-of", required=True, help="yyyy-MM-dd, used for break aging")
    a = p.parse_args()
    spark = build_spark()
    try:
        print(json.dumps(run(spark, a.input, a.output, a.as_of)))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
