import os, psycopg
c = psycopg.connect(os.environ["DATABASE_URL"])
rows = dict(c.execute("select name, sum(value)::float8 from metrics m join runs r on r.id=m.run_id where r.gh_run_id in ('38927201180','38972566265') group by 1").fetchall())
g = lambda k: rows.get(k, 0)
n = g("n_tenders")
print("tenders", n, "| captcha ok/fail", g("captcha_ok"), g("captcha_fail"), "| sessions_new", g("sessions_new"), "| n_http", g("n_http"))
for k in ("t_list_s", "t_fetch_s", "t_docs_s", "t_parse_s", "t_upload_s", "t_captcha_cnn_s", "t_http_s", "t_gate_wait_s"):
    print(f"{k:16s} {g(k):>12,.0f} thread-s   {g(k)/n:6.2f} s/tender")
att = g("captcha_ok") + g("captcha_fail")
print("captcha attempts", att, "per tender", round(att/n, 3), "; failed share", round(g("captcha_fail")/att*100), "%")
print("cnn model cpu s per attempt:", round(g("t_captcha_cnn_s")/max(1, g("n_captcha_cnn")), 3))
