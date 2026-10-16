"""Health rules. Pure functions over a stats dict so they can be tested without a database.

Each rule returns (name, level, message) where level is 'ok' | 'warn' | 'bad'.
"""

ORDER = {"ok": 0, "warn": 1, "bad": 2}


def evaluate(s: dict) -> dict:
    """s keys (per project):
      running, stalled        run counts (stalled = running with no activity past STALL_AFTER_S)
      expected_runners        distinct job_index values seen for the newest workflow run, vs total_jobs
      seen_runners
      rate_now, rate_prev     records/min over the last 5 min and the 5 min before that
      batches_5m, errors_5m, blocked_5m, records_5m
      queue                   {'pending':n,'claimed':n,'done':n,'failed':n}
    """
    rules = []

    if s.get("failed_runs_1h"):
        rules.append(("runs", "warn", f"{s['failed_runs_1h']} run(s) failed in the last hour"))

    if s["running"] == 0:
        rules.append(("activity", "ok", "idle: no runs in progress"))
    else:
        if s["stalled"]:
            lvl = "bad" if s["stalled"] == s["running"] else "warn"
            rules.append(("heartbeat", lvl, f"{s['stalled']} of {s['running']} running jobs silent past the stall limit"))
        else:
            rules.append(("heartbeat", "ok", f"all {s['running']} running jobs reporting"))

        if s.get("expected_runners") and s["seen_runners"] < s["expected_runners"]:
            rules.append(("coverage", "warn", f"{s['seen_runners']} of {s['expected_runners']} runner slices reporting"))
        elif s.get("expected_runners"):
            rules.append(("coverage", "ok", f"all {s['expected_runners']} runner slices reporting"))

        prev, now = s["rate_prev"], s["rate_now"]
        if prev >= 5 and now < prev * 0.3:
            rules.append(("throughput", "bad" if now == 0 else "warn", f"records/min fell from {prev:.0f} to {now:.0f}"))
        else:
            rules.append(("throughput", "ok", f"{now:.0f} records/min"))

        b = s["batches_5m"]
        if b >= 5:
            err = s["errors_5m"] / max(1, s["records_5m"] + s["errors_5m"])
            if err > 0.25:
                rules.append(("errors", "bad", f"{err:.0%} of work failing in the last 5 min"))
            elif err > 0.08:
                rules.append(("errors", "warn", f"{err:.0%} of work failing in the last 5 min"))
            else:
                rules.append(("errors", "ok", f"{err:.1%} error rate"))
            blk = s["blocked_5m"] / max(1, s["records_5m"] + s["blocked_5m"])
            if blk > 0.25:
                rules.append(("blocking", "bad", f"{blk:.0%} of requests blocked or captcha-failed"))
            elif blk > 0.08:
                rules.append(("blocking", "warn", f"{blk:.0%} of requests blocked or captcha-failed"))

    q = s.get("queue") or {}
    total = sum(q.values())
    if total:
        if q.get("failed", 0) and q["failed"] / total > 0.02:
            rules.append(("queue", "warn", f"{q['failed']} of {total} queue items failed"))
        else:
            left = q.get("pending", 0) + q.get("claimed", 0)
            rules.append(("queue", "ok", f"{q.get('done', 0)} of {total} done" + (f", {left} waiting for the next run" if left and s["running"] == 0 else "")))

    worst = max((ORDER[r[1]] for r in rules), default=0)
    verdict = {0: "healthy", 1: "degraded", 2: "unhealthy"}[worst]
    return {"verdict": verdict, "rules": [{"name": n, "level": l, "message": m} for n, l, m in rules]}
