"""Check job counts against the generator's exact defect counts. Exits non-zero on mismatch.

    python scripts/verify_e2e.py gen_stats.json job_stats.json
"""
import json
import sys

gen = json.load(open(sys.argv[1]))
job = json.load(open(sys.argv[2]))
expected = {
    "quarantined": gen["malformed"],
    "valid": job["raw"] - gen["malformed"],
    "after_dedupe": job["valid"] - gen["redelivered"],
    "matched_pairs": gen["pairs"] - gen["missing_b"] - gen["amount_diff"],
    "breaks": gen["missing_b"] + 2 * gen["amount_diff"],
}
bad = {k: (job[k], v) for k, v in expected.items() if job[k] != v}
print("job:", job)
print("expected:", expected)
if bad:
    sys.exit(f"MISMATCH (actual, expected): {bad}")
print("end-to-end counts match")
