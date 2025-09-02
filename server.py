# server.py
import os
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv
from flask import Flask, request, jsonify
from flask_cors import CORS

# Load .env from this folder (back/.env) before other imports that read env
ENV_PATH = Path(__file__).with_name(".env")
load_dotenv(dotenv_path=ENV_PATH)

# Optional sanity logging
k = (os.getenv("QDRANT_API_KEY") or "").strip()
print(f"[boot] QDRANT_URL={os.getenv('QDRANT_URL')}")
print(f"[boot] QDRANT_API_KEY prefix={k[:8]} len={len(k)}")

from openai_integration import embed_text, chat_answer
from qdrant_rest import search

# Config
PORT = int(os.getenv("PORT", "8000"))
CORS_ORIGINS = [o.strip() for o in os.getenv("CORS_ORIGINS", "http://localhost:3000").split(",")]
SHOW_SOURCES = (os.getenv("SHOW_SOURCES", "false").strip().lower() == "true")
TOP_K = int(os.getenv("TOP_K", "12"))
MAX_CONTEXT_CHARS = int(os.getenv("MAX_CONTEXT_CHARS", "12000"))

app = Flask(__name__)
CORS(app, origins=CORS_ORIGINS)

DATA_DIR = Path(__file__).with_name("data")
HASHTAGS_CSV = DATA_DIR / "instagram_hashtags.csv"
GA_CSV = DATA_DIR / "ga_metrics.csv"


# ---------------------------
# Hashtag fallback (CSV)
# ---------------------------
def _hashtag_fallback() -> str:
    """
    Return bullet lines from instagram_hashtags.csv if present, else empty string.
    Expected columns: hashtag, freq (or count).
    """
    if not HASHTAGS_CSV.exists():
        return ""
    bullets = []
    with HASHTAGS_CSV.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            tag = (row.get("hashtag") or "").strip()
            if not tag:
                continue
            freq = (row.get("freq") or row.get("count") or "").strip()
            bullets.append(f"- {tag} (frequency: {freq})" if freq else f"- {tag}")
    return "\n".join(bullets)


# ---------------------------
# GA fallback (CSV)
# ---------------------------
def _parse_int(val, default=0):
    try:
        if val is None:
            return default
        s = str(val).strip().replace(",", "")
        if s == "":
            return default
        return int(float(s))
    except Exception:
        return default

def _find_col(row: dict, names: list[str]) -> str:
    """Return the first matching column name in row (case-insensitive)."""
    lower_map = {k.lower(): k for k in row.keys()}
    for n in names:
        key = lower_map.get(n.lower())
        if key:
            return key
    return ""

def _load_ga_rows():
    """
    Load ga_metrics.csv into a list of normalized dicts:
    {date: date, country: str|None, page: str|None, users: int, events: int}
    Accepts multiple plausible schema variants.
    """
    out = []
    if not GA_CSV.exists():
        return out

    with GA_CSV.open("r", encoding="utf-8", newline="") as f:
        rdr = csv.DictReader(f)
        headers = [h.strip() for h in (rdr.fieldnames or [])]
        if not headers:
            return out

        for raw in rdr:
            if not raw:
                continue
            # Columns we might see
            date_col   = _find_col(raw, ["date", "Date"])
            country_col= _find_col(raw, ["country", "Country"])
            page_col   = _find_col(raw, ["pagePath", "page", "page_title", "Page path and screen class", "pagePathPlusQuery"])
            users_col  = _find_col(raw, ["activeUsers", "users", "Users"])
            events_col = _find_col(raw, ["eventCount", "Events"])

            # Date parse (GA often returns YYYYMMDD)
            dt = None
            if date_col:
                ds = (raw.get(date_col) or "").strip()
                if len(ds) == 8 and ds.isdigit():
                    # format YYYYMMDD
                    try:
                        dt = datetime.strptime(ds, "%Y%m%d").date()
                    except Exception:
                        dt = None
                else:
                    # try more general parse
                    try:
                        dt = datetime.fromisoformat(ds).date()
                    except Exception:
                        dt = None

            country = (raw.get(country_col) or "").strip() if country_col else None
            page    = (raw.get(page_col) or "").strip() if page_col else None
            users   = _parse_int(raw.get(users_col), 0) if users_col else 0
            events  = _parse_int(raw.get(events_col), 0) if events_col else 0

            out.append({"date": dt, "country": country or None, "page": page or None, "users": users, "events": events})

    return out

def _ga_in_window(rows, days: int):
    """Filter rows to last N days (based on max date present)."""
    valid = [r for r in rows if r["date"] is not None]
    if not valid:
        return []
    latest = max(r["date"] for r in valid)
    start = latest - timedelta(days=days - 1)
    return [r for r in valid if start <= r["date"] <= latest]

def _ga_summary(rows, days: int):
    w = _ga_in_window(rows, days)
    total_users = sum(r["users"] for r in w)
    total_events = sum(r["events"] for r in w)
    # daily breakdown (users)
    by_day = {}
    for r in w:
        by_day[r["date"]] = by_day.get(r["date"], 0) + r["users"]
    # format
    day_lines = []
    for d in sorted(by_day.keys()):
        day_lines.append(f"- {d.isoformat()}: {by_day[d]} users")
    return (
        f"**Google Analytics — last {days} days (from ingested GA data)**\n"
        f"- Total active users: **{total_users}**\n"
        f"- Total events: **{total_events}**\n"
        f"- Daily users:\n" + ("\n".join(day_lines) if day_lines else "- (no daily rows)")
    )

def _ga_top_countries(rows, days: int, limit: int = 5):
    w = _ga_in_window(rows, days)
    agg = {}
    for r in w:
        key = r["country"] or "(unknown)"
        agg[key] = agg.get(key, 0) + r["users"]
    top = sorted(agg.items(), key=lambda x: x[1], reverse=True)[:limit]
    if not top:
        return "I don’t have GA country data in the current dataset."
    lines = [f"**Top countries by users — last {days} days**"]
    for c, n in top:
        lines.append(f"- {c}: {n}")
    return "\n".join(lines)

def _ga_top_pages(rows, days: int, limit: int = 5):
    w = _ga_in_window(rows, days)
    agg = {}
    for r in w:
        key = r["page"] or "(unknown)"
        agg[key] = agg.get(key, 0) + r["users"]
    top = sorted(agg.items(), key=lambda x: x[1], reverse=True)[:limit]
    if not top:
        return "I don’t have GA page data in the current dataset."
    lines = [f"**Top pages by users — last {days} days**"]
    for p, n in top:
        lines.append(f"- {p}: {n}")
    return "\n".join(lines)

def _ga_daily_users(rows, days: int):
    w = _ga_in_window(rows, days)
    by_day = {}
    for r in w:
        by_day[r["date"]] = by_day.get(r["date"], 0) + r["users"]
    lines = [f"**Daily active users — last {days} days**"]
    for d in sorted(by_day.keys()):
        lines.append(f"- {d.isoformat()}: {by_day[d]}")
    if len(lines) == 1:
        return "I don’t have GA daily user data in the current dataset."
    return "\n".join(lines)

def _maybe_answer_ga(q_lower: str) -> str | None:
    """
    Detect GA intent and answer from ga_metrics.csv, bypassing the LLM.
    Supported patterns:
      - "ga summary" / "google analytics summary" + "last 7/30 days"
      - "top countries" (+ last X days)
      - "top pages" (+ last X days)
      - "daily active users" (+ last X days)
    """
    if not GA_CSV.exists():
        return None

    # Detect GA intent
    ga_mention = ("ga" in q_lower) or ("google analytics" in q_lower) or ("analytics" in q_lower)
    if not ga_mention and not any(kw in q_lower for kw in ["top countries", "top pages", "daily active users", "daily users"]):
        return None

    # Determine window
    days = 7
    if "30" in q_lower or "last month" in q_lower:
        days = 30
    elif "7" in q_lower or "week" in q_lower:
        days = 7

    rows = _load_ga_rows()
    if not rows:
        return None

    if "top countries" in q_lower:
        return _ga_top_countries(rows, days)
    if "top pages" in q_lower:
        return _ga_top_pages(rows, days)
    if "daily active users" in q_lower or "daily users" in q_lower:
        return _ga_daily_users(rows, days)
    if "summary" in q_lower or ("ga" in q_lower and "summary" in q_lower):
        return _ga_summary(rows, days)

    # Generic GA ask -> sensible default summary
    return _ga_summary(rows, days)


@app.get("/status")
def status():
    return jsonify({"ok": True})


@app.post("/ask")
def ask():
    try:
        data = request.get_json(force=True) or {}
        q = (data.get("question") or "").strip()
        if not q:
            return jsonify({"error": "Missing question"}), 400

        q_lower = q.lower()

        # 0) SPECIAL CASE: Instagram hashtags -> guaranteed CSV fallback
        if "hashtag" in q_lower or q_lower.startswith("#") or "hashtags" in q_lower:
            fallback = _hashtag_fallback()
            if fallback:
                return jsonify({"answer": fallback, "sources": []})

        # 0.1) SPECIAL CASE: Google Analytics -> read ga_metrics.csv directly
        ga_answer = _maybe_answer_ga(q_lower)
        if ga_answer:
            return jsonify({"answer": ga_answer, "sources": []})

        # 1) Embed the question
        qvec = embed_text(q)

        # 2) Vector search (wider to reduce false 'unknown')
        hits = search(qvec, top_k=TOP_K)

        # 3) Build context + collect unique sources (internal use only)
        chunks = [h.get("payload", {}).get("text", "") for h in hits if h.get("payload")]
        context = "\n\n---\n\n".join([c for c in chunks if c])[:MAX_CONTEXT_CHARS]

        sources = list(
            dict.fromkeys(
                [h.get("payload", {}).get("source", "") for h in hits if h.get("payload")]
            )
        )

        # 4) If we truly have no context, short friendly fallback (avoid wasting model call)
        if not context.strip():
            final_ans = "I don’t know from the current dataset."
            response = {"answer": final_ans, "sources": sources if SHOW_SOURCES else []}
            return jsonify(response)

        # 5) LLM answer (grounded with context)
        raw_ans = (chat_answer(context, q, temperature=0.2) or "").strip()

        # 6) Return exactly what the model answered (no forced closer line)
        final_ans = raw_ans

        # 7) Response shape (sources only in metadata if enabled)
        response = {"answer": final_ans}
        response["sources"] = sources if SHOW_SOURCES else []

        return jsonify(response)

    except Exception as e:
        # Keep frontend stable; console will show full traceback
        return jsonify({"error": f"{type(e).__name__}: {e}", "answer": "", "sources": []}), 500


if __name__ == "__main__":
    # Debug=True for local dev; switch off for prod
    app.run(host="0.0.0.0", port=PORT, debug=True)
