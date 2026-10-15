"""Pure DataFrame transforms for the trade-events pipeline.

Input events are JSON lines: streamId, eventId, side (A|B), reference, amount, currency, date.
Everything here takes and returns DataFrames (no I/O), so it is testable on in-memory data.
"""
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

RAW_SCHEMA = StructType([StructField(n, StringType(), True) for n in
                         ("streamId", "eventId", "side", "reference", "amount", "currency", "date")])


def parse_events(raw: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Cast to typed columns. Returns (valid, quarantine); quarantine has a `reason` column.

    A record is invalid if a required field is missing, side is not A/B, amount is not numeric,
    the date is not ISO yyyy-MM-dd, or the currency is not 3 letters.
    """
    typed = (raw
             .withColumn("side", F.upper(F.trim("side")))
             .withColumn("reference", F.upper(F.trim("reference")))
             .withColumn("currency", F.upper(F.trim("currency")))
             .withColumn("amount_n", F.col("amount").cast("decimal(19,4)"))
             .withColumn("trade_date", F.to_date("date", "yyyy-MM-dd")))
    reason = (F.when(F.col("streamId").isNull() | F.col("eventId").isNull() | F.col("reference").isNull(), "missing field")
              .when(~F.col("side").isin("A", "B"), "bad side")
              .when(F.col("amount_n").isNull(), "bad amount")
              .when(F.col("trade_date").isNull(), "bad date")
              .when(~F.col("currency").rlike("^[A-Z]{3}$"), "bad currency"))
    flagged = typed.withColumn("reason", reason)
    valid = (flagged.filter(F.col("reason").isNull())
             .select("streamId", "eventId", "side", "reference",
                     F.col("amount_n").alias("amount"), "currency", "trade_date"))
    quarantine = flagged.filter(F.col("reason").isNotNull()).select(*raw.columns, "reason")
    return valid, quarantine


def quarantine_summary(quarantine: DataFrame) -> DataFrame:
    """Data-quality view: how many rejected records per reason."""
    return quarantine.groupBy("reason").agg(F.count("*").alias("records")).orderBy(F.desc("records"), "reason")


def dedupe(events: DataFrame) -> DataFrame:
    """At-least-once delivery means repeats. Keep one row per (streamId, eventId)."""
    return events.dropDuplicates(["streamId", "eventId"])


def exact_reconcile(events: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Pair side A with side B one-to-one on (streamId, reference, currency, amount, trade_date).

    Duplicates pair in eventId order, so n copies on A and m on B give min(n, m) matches.
    Returns (matched, unmatched); unmatched keeps its side.
    """
    key = ["streamId", "reference", "currency", "amount", "trade_date"]
    w = Window.partitionBy(*key, "side").orderBy("eventId")
    ranked = events.withColumn("rn", F.row_number().over(w))
    a = ranked.filter("side = 'A'").select(*key, "rn", F.col("eventId").alias("a_event"))
    b = ranked.filter("side = 'B'").select(*key, "rn", F.col("eventId").alias("b_event"))
    matched = a.join(b, key + ["rn"], "inner").drop("rn")
    # event ids are only unique within a stream, so the anti-join must use both columns
    used = (matched.select("streamId", F.col("a_event").alias("eventId"))
            .union(matched.select("streamId", F.col("b_event").alias("eventId"))))
    unmatched = events.join(used, ["streamId", "eventId"], "left_anti")
    return matched, unmatched


BREAK_TYPES = ("MISSING_A", "MISSING_B", "CURRENCY_MISMATCH", "DATE_MISMATCH", "AMOUNT_MISMATCH", "WITHIN_TOLERANCE")


def classify_breaks(unmatched: DataFrame, tolerance: str = "0.05") -> DataFrame:
    """Explain each break by pairing leftover A and B lines on reference alone (one-to-one, eventId order).

    A paired line is CURRENCY_MISMATCH, then DATE_MISMATCH, else AMOUNT_MISMATCH (WITHIN_TOLERANCE if the absolute amount
    difference is at most `tolerance`, which is reported but not treated as a match). An unpaired A line has no counterpart
    on B (MISSING_B); an unpaired B line is MISSING_A. `exposure` is the amount difference for amount breaks and the line's
    absolute amount otherwise. One row per break.
    """
    w = Window.partitionBy("reference", "side").orderBy("streamId", "eventId")
    ranked = unmatched.withColumn("rn", F.row_number().over(w))

    def side(s: str, p: str) -> DataFrame:
        return (ranked.filter(F.col("side") == s)
                .select("reference", "rn",
                        F.col("eventId").alias(f"{p}_event"), F.col("amount").alias(f"{p}_amount"),
                        F.col("currency").alias(f"{p}_ccy"), F.col("trade_date").alias(f"{p}_date")))

    j = side("A", "a").join(side("B", "b"), ["reference", "rn"], "full_outer")
    diff = F.abs(F.col("b_amount") - F.col("a_amount"))
    break_type = (F.when(F.col("a_event").isNull(), "MISSING_A")
                  .when(F.col("b_event").isNull(), "MISSING_B")
                  .when(F.col("a_ccy") != F.col("b_ccy"), "CURRENCY_MISMATCH")
                  .when(F.col("a_date") != F.col("b_date"), "DATE_MISMATCH")
                  .when(diff <= F.lit(tolerance).cast("decimal(19,4)"), "WITHIN_TOLERANCE")
                  .otherwise("AMOUNT_MISMATCH"))
    exposure = F.when(F.col("break_type").isin("AMOUNT_MISMATCH", "WITHIN_TOLERANCE"), diff) \
        .otherwise(F.abs(F.coalesce(F.col("a_amount"), F.col("b_amount"))))
    return (j.withColumn("break_type", break_type)
            .withColumn("currency", F.coalesce("a_ccy", "b_ccy"))
            .withColumn("trade_date", F.coalesce("a_date", "b_date"))
            .withColumn("exposure", exposure)
            .select("reference", "break_type", "a_event", "b_event", "a_amount", "b_amount", "currency", "trade_date",
                    "exposure"))


def break_type_summary(spark: SparkSession, breaks: DataFrame) -> DataFrame:
    """Breaks and exposure per break type and currency (Spark SQL), largest exposure first."""
    breaks.createOrReplaceTempView("classified_breaks")
    return spark.sql("""
        SELECT break_type, currency, COUNT(*) AS breaks, SUM(exposure) AS exposure
        FROM classified_breaks
        GROUP BY break_type, currency
        ORDER BY exposure DESC, break_type, currency""")


def daily_break_summary(spark: SparkSession, matched: DataFrame, unmatched: DataFrame) -> DataFrame:
    """Per trade date and currency: matched count, unmatched count, break rate, unmatched value (Spark SQL)."""
    matched.createOrReplaceTempView("matched")
    unmatched.createOrReplaceTempView("unmatched")
    return spark.sql("""
        WITH m AS (
            SELECT trade_date, currency, COUNT(*) AS matched_pairs
            FROM matched GROUP BY trade_date, currency),
        u AS (
            SELECT trade_date, currency, COUNT(*) AS unmatched_lines, SUM(ABS(amount)) AS unmatched_value
            FROM unmatched GROUP BY trade_date, currency)
        SELECT COALESCE(m.trade_date, u.trade_date) AS trade_date,
               COALESCE(m.currency, u.currency) AS currency,
               COALESCE(m.matched_pairs, 0) AS matched_pairs,
               COALESCE(u.unmatched_lines, 0) AS unmatched_lines,
               COALESCE(u.unmatched_value, CAST(0 AS DECIMAL(19,4))) AS unmatched_value,
               ROUND(COALESCE(u.unmatched_lines, 0) * 1.0
                     / (COALESCE(u.unmatched_lines, 0) + 2 * COALESCE(m.matched_pairs, 0)), 4) AS break_rate
        FROM m FULL OUTER JOIN u ON m.trade_date = u.trade_date AND m.currency = u.currency
        ORDER BY trade_date, currency""")


def aging(unmatched: DataFrame, as_of: str) -> DataFrame:
    """Open breaks by age bucket relative to `as_of` (yyyy-MM-dd): 0-1, 2-7, 8-30, 30+ days."""
    age = F.datediff(F.to_date(F.lit(as_of)), F.col("trade_date"))
    bucket = (F.when(age <= 1, "0-1d").when(age <= 7, "2-7d").when(age <= 30, "8-30d").otherwise("30d+"))
    return (unmatched.withColumn("bucket", bucket)
            .groupBy("bucket").agg(F.count("*").alias("lines"), F.sum(F.abs("amount")).alias("value"))
            .orderBy("bucket"))
