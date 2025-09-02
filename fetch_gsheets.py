# fetch_gsheets.py
import os
import re
import csv
import json
import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

load_dotenv()

SHEET_URLS = [s.strip() for s in os.getenv("SHEET_URLS", "").split(",") if s.strip()]
REQUESTS_VERIFY_SSL = os.getenv("REQUESTS_VERIFY_SSL", "true").strip().lower() != "false"
TIMEOUT_SECS = int(os.getenv("REQUEST_TIMEOUT_SECS", "40"))

# optional: comma-separated list of column names you expect (case-insensitive)
HASHTAG_COLUMNS = [c.strip().lower() for c in os.getenv("HASHTAG_COLUMNS", "").split(",") if c.strip()]

def _valid_proxy(url: str | None) -> str | None:
    if not url: return None
    u = url.strip().lower()
    if "your.proxy" in u or u.endswith(":port") or "://" not in u: return None
    if not (u.startswith("http://") or u.startswith("https://")): return None
    return url.strip()

HTTPS_PROXY_RAW = os.getenv("HTTPS_PROXY", "")
HTTP_PROXY_RAW  = os.getenv("HTTP_PROXY", "")
PROXIES = {}
hp = _valid_proxy(HTTP_PROXY_RAW)
sp = _valid_proxy(HTTPS_PROXY_RAW)
if hp: PROXIES["http"] = hp
if sp: PROXIES["https"] = sp

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; DataDepotFetcher/1.2)"}

def _session() -> requests.Session:
    s = requests.Session()
    retry = Retry(total=5, backoff_factor=0.8,
                  status_forcelist=(429, 500, 502, 503, 504),
                  allowed_methods=("GET","HEAD"), raise_on_status=False)
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("https://", adapter); s.mount("http://", adapter)
    if PROXIES: s.proxies.update(PROXIES)
    s.headers.update(HEADERS)
    return s

def export_csv_text(url: str):
    m = re.search(r"/spreadsheets/d/([A-Za-z0-9-_]+)", url)
    if not m:
        print("[sheets] Bad sheet URL:", url); return [], []
    sid = m.group(1)
    export = f"https://docs.google.com/spreadsheets/d/{sid}/export?format=csv"
    s = _session()
    try:
        r = s.get(export, timeout=TIMEOUT_SECS, verify=REQUESTS_VERIFY_SSL, allow_redirects=True)
        if r.status_code != 200:
            print("[sheets] Failed export (not public?)", url, r.status_code); return [], []
        lines = r.text.splitlines()
        rows = list(csv.reader(lines))
        headers = rows[0] if rows else []
        data = rows[1:] if len(rows) > 1 else []
        return headers, data
    except requests.RequestException as e:
        print(f"[sheets] Network error for {url}: {e}"); return [], []

def row_to_text(headers, row):
    if headers:
        parts = []
        for h, v in zip(headers, row):
            v = (v or "").strip()
            if v: parts.append(f"{h.strip()}: {v}")
        return " | ".join(parts) if parts else " | ".join([c.strip() for c in row if (c or '').strip()])
    return " | ".join([c.strip() for c in row if (c or '').strip()])

# --- hashtag helpers ---------------------------------------------------------

HASHTAG_RE = re.compile(r"#([A-Za-z0-9_]+)")

def _collect_hashtags_from_text(text: str) -> list[str]:
    # return raw hashtags with leading '#', de-duplicated within this text
    tags = set()
    for m in HASHTAG_RE.finditer(text or ""):
        tag = "#" + m.group(1)
        tags.add(tag)
    return list(tags)

def _collect_hashtags(headers, data_rows):
    """
    Strategy:
      1) If specific HASHTAG_COLUMNS are set, scan only those headers (if present).
      2) Otherwise, scan ALL cells of every row for #tags.
      3) Sum frequencies across sheets and rows.
    """
    freq: dict[str, int] = {}

    # Map header name -> index
    hdr_idx = { (h or "").strip().lower(): i for i, h in enumerate(headers or []) }

    # Choose which column indices to scan (if provided)
    indices_to_scan = set()
    if HASHTAG_COLUMNS and headers:
        for want in HASHTAG_COLUMNS:
            if want in hdr_idx:
                indices_to_scan.add(hdr_idx[want])

    for row in data_rows:
        texts = []
        if indices_to_scan:
            for i in indices_to_scan:
                if i < len(row) and (row[i] or "").strip():
                    texts.append(row[i])
        else:
            # scan all cells
            texts.extend([c for c in row if (c or "").strip()])

        # gather unique tags in this row
        row_tags = set()
        for t in texts:
            for tag in _collect_hashtags_from_text(t):
                row_tags.add(tag)

        for tag in row_tags:
            freq[tag] = freq.get(tag, 0) + 1

    # convert to sorted list
    items = [{"hashtag": k, "frequency": v} for k, v in freq.items()]
    items.sort(key=lambda x: x["frequency"], reverse=True)
    return items

# --- main --------------------------------------------------------------------

def main():
    rows_out = []
    all_hashtags = []

    for url in SHEET_URLS:
        headers, data = export_csv_text(url)
        if not data:
            print("[sheets] No rows parsed for", url); continue

        # RAG rows
        for idx, r in enumerate(data, 1):
            text = row_to_text(headers, r)
            if text:
                rows_out.append({"source": f"{url}#row={idx}", "text": text})

        # Hashtags by scanning columns or all cells
        tags = _collect_hashtags(headers, data)
        if tags:
            print(f"[sheets] Found {len(tags)} unique hashtags in", url)
            all_hashtags.extend(tags)
        else:
            print(f"[sheets] No hashtags found in {url}")

        print(f"Indexed sheet: {url} rows={len(data)}")

    os.makedirs("data", exist_ok=True)

    # Save corpus for RAG
    out_csv = os.path.join("data", "gsheets_corpus.csv")
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["source", "text"])
        w.writeheader()
        for r in rows_out:
            w.writerow(r)
    print("Saved:", out_csv, "rows:", len(rows_out))

    # Merge & save hashtags (sum same tags across sheets)
    out_json = os.path.join("data", "instagram_hashtags.json")
    if all_hashtags:
        agg: dict[str, int] = {}
        for item in all_hashtags:
            tag = item["hashtag"]
            agg[tag] = agg.get(tag, 0) + int(item.get("frequency") or 0)
        final = [{"hashtag": t, "frequency": f} for t, f in agg.items()]
        final.sort(key=lambda x: x["frequency"], reverse=True)
        with open(out_json, "w", encoding="utf-8") as jf:
            json.dump(final, jf, ensure_ascii=False, indent=2)
        print("[sheets] Saved hashtag cache:", out_json, "items:", len(final))
    else:
        with open(out_json, "w", encoding="utf-8") as jf:
            json.dump([], jf)
        print("[sheets] No hashtags detected; wrote empty", out_json)

if __name__ == "__main__":
    main()
