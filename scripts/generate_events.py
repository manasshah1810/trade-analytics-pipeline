"""Generate synthetic, seeded event data (JSON lines) with realistic defects.

    python scripts/generate_events.py --out data/raw --pairs 100000 --seed 7

Per pair: both sides usually present; some are missing on one side, some amounts differ (by a cent, or clearly), and a few
events are redelivered or malformed. Counts printed at the end are exact, so tests and the README can quote them.
"""
import argparse
import json
import os
import random
from datetime import date, timedelta


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--pairs", type=int, default=1000)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--files", type=int, default=4)
    a = ap.parse_args()
    rnd = random.Random(a.seed)
    os.makedirs(a.out, exist_ok=True)
    handles = [open(os.path.join(a.out, f"events_{i}.jsonl"), "w", encoding="utf-8") for i in range(a.files)]
    start = date(2026, 3, 1)
    stats = dict(pairs=a.pairs, missing_b=0, amount_diff=0, small_diff=0, redelivered=0, malformed=0)
    n = 0

    def emit(rec):
        handles[n % a.files].write(json.dumps(rec) + "\n")

    for i in range(a.pairs):
        d = (start + timedelta(days=rnd.randrange(30))).isoformat()
        ccy = rnd.choice(["USD", "EUR", "INR", "GBP"])
        amt = f"{rnd.uniform(1, 50000):.2f}"
        base = {"streamId": d, "reference": f"TRD-{i:08d}", "amount": amt, "currency": ccy, "date": d}
        ea = {**base, "eventId": f"a{i}", "side": "A"}
        emit(ea); n += 1
        r = rnd.random()
        if r < 0.03:
            stats["missing_b"] += 1
        else:
            eb = {**base, "eventId": f"b{i}", "side": "B"}
            if r < 0.045:  # one cent off: within the default 0.05 tolerance
                eb["amount"] = f"{float(amt) + 0.01:.2f}"
                stats["small_diff"] += 1
            elif r < 0.06:  # clearly wrong amount (reuses r's band so the random stream, and event counts, are unchanged)
                eb["amount"] = f"{float(amt) + 5 + (i % 50) * 7.37:.2f}"
                stats["amount_diff"] += 1
            emit(eb); n += 1
        if rnd.random() < 0.01:
            emit(ea); n += 1; stats["redelivered"] += 1
        if rnd.random() < 0.005:
            emit({**base, "eventId": f"x{i}", "side": "A", "amount": "not-a-number"}); n += 1
            stats["malformed"] += 1
    for h in handles:
        h.close()
    print(json.dumps(stats))


if __name__ == "__main__":
    main()
