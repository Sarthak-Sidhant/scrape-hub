from scrape_hub.health import evaluate

BASE = dict(running=0, stalled=0, expected_runners=0, seen_runners=0, rate_now=0, rate_prev=0,
            batches_5m=0, errors_5m=0, blocked_5m=0, records_5m=0, queue={})


def rules(**kw):
    return {r["name"]: r["level"] for r in evaluate({**BASE, **kw})["rules"]}


def test_idle_is_healthy():
    assert evaluate(BASE)["verdict"] == "healthy"


def test_all_stalled_is_unhealthy():
    out = evaluate({**BASE, "running": 4, "stalled": 4})
    assert out["verdict"] == "unhealthy"


def test_partial_stall_degraded():
    assert rules(running=4, stalled=1)["heartbeat"] == "warn"


def test_missing_runners_warns():
    assert rules(running=3, expected_runners=20, seen_runners=14)["coverage"] == "warn"


def test_throughput_collapse():
    assert rules(running=2, rate_prev=100, rate_now=0)["throughput"] == "bad"
    assert rules(running=2, rate_prev=100, rate_now=20)["throughput"] == "warn"
    assert rules(running=2, rate_prev=100, rate_now=90)["throughput"] == "ok"


def test_error_and_block_rates():
    r = rules(running=2, batches_5m=10, records_5m=60, errors_5m=40)
    assert r["errors"] == "bad"
    r = rules(running=2, batches_5m=10, records_5m=60, blocked_5m=40)
    assert r["blocking"] == "bad"


def test_queue_left_but_idle_is_not_a_warning():
    out = evaluate({**BASE, "queue": {"pending": 5, "done": 5}})
    assert out["verdict"] == "healthy"


def test_failed_runs_warn():
    assert rules(failed_runs_1h=2)["runs"] == "warn"


def test_queue_done_ok():
    assert rules(queue={"done": 10})["queue"] == "ok"
