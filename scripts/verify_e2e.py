"""Check job counts against the generator's exact defect counts. Exits non-zero on mismatch.

    python scripts/verify_e2e.py gen_stats.json job_stats.json

Run the job with the default tolerance (0.05) so one-cent differences classify as WITHIN_TOLERANCE.
"""
import json
import sys

gen = json.load(open(sys.argv[1]))
job = json.load(open(sys.argv[2]))
mismatched = gen["amount_diff"] + gen["small_diff"]
expected = {
    "quarantined": gen["malformed"],
    "valid": job["raw"] - gen["malformed"],
    "after_dedupe": job["valid"] - gen["redelivered"],
    "matched_pairs": gen["pairs"] - gen["missing_b"] - mismatched,
    "breaks": gen["missing_b"] + 2 * mismatched,
}
expected_types = {
    "MISSING_B": gen["missing_b"],
    "MISSING_A": 0,
    "AMOUNT_MISMATCH": gen["amount_diff"],
    "WITHIN_TOLERANCE": gen["small_diff"],
    "CURRENCY_MISMATCH": 0,
    "DATE_MISMATCH": 0,
}
bad = {k: (job[k], v) for k, v in expected.items() if job[k] != v}
bad.update({f"type {k}": (job["break_types"][k], v) for k, v in expected_types.items() if job["break_types"][k] != v})
print("job:", job)
print("expected:", expected, expected_types)
if bad:
    sys.exit(f"MISMATCH (actual, expected): {bad}")
print("end-to-end counts match")
