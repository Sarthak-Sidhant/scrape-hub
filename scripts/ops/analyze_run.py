import os, sys, psycopg
gh, procs = sys.argv[1], int(sys.argv[2])
db = psycopg.connect(os.environ["DATABASE_URL"]); q = lambda s, *a: db.execute(s, a).fetchall()
runs = q("select id, job_index, state, records, extract(epoch from (coalesce(finished_at,last_seen)-started_at)) from runs where gh_run_id=%s order by job_index", gh)
M = {r[0]: {n: float(v) for n, v in q("select name, sum(value) from metrics where run_id=%s group by 1", r[0])} for r in runs}
by_runner = {}
for rid, idx, st, rec, secs in runs:
    by_runner.setdefault(idx // procs, []).append((rid, idx, st, rec, secs))
print(f"{len(runs)} processes on {len(by_runner)} runners ({procs} processes each); states: {sorted({r[2] for r in runs})}")
tot_rec = 0; rates = []
for runner, ps in sorted(by_runner.items()):
    rec = sum(p[3] for p in ps); span = max(p[4] for p in ps) / 60
    cpu = sum(M[p[0]].get("cpu_s", 0) for p in ps); wall = max(M[p[0]].get("wall_s", 0) for p in ps); cores = max(M[p[0]].get("cores", 4) for p in ps)
    per_proc = [p[3] / (p[4] / 60) for p in ps if p[4] > 0]
    tot_rec += rec; rates.append(rec / span)
    print(f"runner {runner}: {rec} tenders in {span:.1f} min = {rec/span:.0f}/min for the whole runner; per process {min(per_proc):.0f} to {max(per_proc):.0f}/min; CPU {cpu/wall*100:.0f}% of one core = {cpu/wall/cores*100:.0f}% of all {int(cores)} cores")
print(f"per runner average: {sum(rates)/len(rates):.0f} tenders/min  (D, 1 process x 16 threads: 77; A: 31)")
agg = {}
for rid, *_ in runs:
    for k, v in M[rid].items(): agg[k] = agg.get(k, 0) + v
n = agg.get("n_tenders", 1) or 1
print("thread-seconds per tender (all processes):", {k[2:-2]: round(agg.get(k, 0) / n, 2) for k in ("t_list_s", "t_fetch_s", "t_docs_s", "t_parse_s", "t_upload_s")})
print(f"portal wait per request: {agg.get('t_http_s',0)/max(agg.get('n_http',1),1)*1000:.0f} ms over {int(agg.get('n_http',0))} requests; captcha ok/fail {int(agg.get('captcha_ok',0))}/{int(agg.get('captcha_fail',0))}; docs stored {int(agg.get('doc_stored',0))} refused {int(agg.get('doc_refused',0))}")
push = q("select name, sum(value) from metrics where run_id = any(%s) and name like 'http_%%' and name not in ('http_error') group by 1", [r[0] for r in runs])
err = q("select coalesce(sum(errors),0), coalesce(sum(blocked),0) from runs where gh_run_id=%s", gh)[0]
print(f"portal refusals (429/403/503...): {dict((k, int(v)) for k, v in push) or 'none'}; connection errors {int(agg.get('http_error',0))}; errors/blocked {err[0]}/{err[1]}")
