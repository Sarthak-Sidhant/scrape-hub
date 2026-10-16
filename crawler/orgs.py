"""Current organisation dropdown from the portal. Its numeric ids are not stable (new organisations shift them),
so queue items are keyed by the id at seed time and verified against the live label before crawling."""
import requests
from bs4 import BeautifulSoup

SEARCH_URL = "https://eprocure.gov.in/eprocure/app?page=WebTenderStatusLists&service=page"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"


def live_options():
    """{org_id: label} for every organisation currently in the search form."""
    s = requests.Session()
    s.headers["User-Agent"] = UA
    s.cookies.set("cookieWorked", "yes", domain="eprocure.gov.in", path="/")
    html = s.get(SEARCH_URL, timeout=30).text
    out = {}
    for sel in BeautifulSoup(html, "html.parser").find_all("select"):
        for o in sel.find_all("option"):
            v = (o.get("value") or "").strip()
            if v.isdigit():
                out[v] = o.text.strip()
    if len(out) < 500:
        raise RuntimeError(f"organisation dropdown looks wrong ({len(out)} options)")
    return out


def norm(name):
    return " ".join(name.split()).casefold()
