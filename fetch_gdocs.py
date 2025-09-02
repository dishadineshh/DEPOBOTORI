# fetch_gdocs.py
import os
import re
import csv
import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

load_dotenv()

DOC_URLS = [s.strip() for s in os.getenv("DOC_URLS", "").split(",") if s.strip()]
DOC_CHAR_LIMIT = int(os.getenv("DOC_CHAR_LIMIT", "0")) or None

REQUESTS_VERIFY_SSL = os.getenv("REQUESTS_VERIFY_SSL", "true").strip().lower() != "false"
TIMEOUT_SECS = int(os.getenv("REQUEST_TIMEOUT_SECS", "40"))

def _valid_proxy(url: str | None) -> str | None:
    if not url:
        return None
    u = url.strip().lower()
    if "your.proxy" in u or u.endswith(":port") or "://" not in u:
        return None
    if not (u.startswith("http://") or u.startswith("https://")):
        return None
    return url.strip()

HTTPS_PROXY_RAW = os.getenv("HTTPS_PROXY", "")
HTTP_PROXY_RAW  = os.getenv("HTTP_PROXY", "")

PROXIES = {}
hp = _valid_proxy(HTTP_PROXY_RAW)
sp = _valid_proxy(HTTPS_PROXY_RAW)
if hp:
    PROXIES["http"] = hp
if sp:
    PROXIES["https"] = sp

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; DataDepotFetcher/1.0)"
}

def _session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=0.8,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "HEAD"),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    if PROXIES:
        s.proxies.update(PROXIES)
    s.headers.update(HEADERS)
    return s

def export_text(url: str) -> str:
    m = re.search(r"document/d/([A-Za-z0-9-_]+)", url)
    if not m:
        print("[docs] Bad doc URL:", url)
        return ""
    doc_id = m.group(1)
    export = f"https://docs.google.com/document/d/{doc_id}/export?format=txt"
    s = _session()
    try:
        r = s.get(export, timeout=TIMEOUT_SECS, verify=REQUESTS_VERIFY_SSL, allow_redirects=True)
        if r.status_code != 200:
            print("[docs] Failed export (not public?)", url, r.status_code)
            return ""
        txt = r.text
        return txt[:DOC_CHAR_LIMIT] if DOC_CHAR_LIMIT else txt
    except requests.RequestException as e:
        print(f"[docs] Network error for {url}: {e}")
        return ""

def main():
    rows = []
    for d in DOC_URLS:
        txt = export_text(d)
        if txt.strip():
            rows.append({"source": d, "text": txt})
            print("Indexed Google Doc:", d, f"(len={len(txt)})")
        else:
            print("Skipped (not public? network blocked?)", d)
    os.makedirs("data", exist_ok=True)
    out = os.path.join("data", "google_docs_corpus.csv")
    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["source", "text"])
        w.writeheader()
        for r in rows:
            w.writerow(r)
    print("Saved:", out, "rows:", len(rows))

if __name__ == "__main__":
    main()
