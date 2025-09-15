# server.py
import os
import re
import csv
from datetime import datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv
from flask import Flask, request, jsonify
from flask_cors import CORS

# Load .env from this folder (back/.env) before other imports that read env
ENV_PATH = Path(__file__).with_name(".env")
load_dotenv(dotenv_path=ENV_PATH)
print(f"[boot] WEB_MODEL={os.getenv('WEB_MODEL')}, WEB_ALLOWED_DOMAINS={os.getenv('WEB_ALLOWED_DOMAINS')}")

# Optional sanity logging
k = (os.getenv("QDRANT_API_KEY") or "").strip()
print(f"[boot] QDRANT_URL={os.getenv('QDRANT_URL')}")
print(f"[boot] QDRANT_API_KEY prefix={k[:8]} len={len(k)}")

from openai_integration import embed_text, chat_answer  # web_answer imported lazily inside /ask
from qdrant_rest import search

# ---------------------------
# Local helpers / config
# ---------------------------
def _env_bool(name: str, default: bool = False) -> bool:
    v = (os.getenv(name) or "").strip().lower()
    if v in ("1", "true", "yes", "on"):
        return True
    if v in ("0", "false", "no", "off"):
        return False
    return default

# Config
PORT = int(os.getenv("PORT", "8000"))
CORS_ORIGINS = [o.strip() for o in os.getenv("CORS_ORIGINS", "http://localhost:3000").split(",")]

app = Flask(__name__)
CORS(app, origins=CORS_ORIGINS)

SHOW_SOURCES = False  # force disable sources
TOP_K = int(os.getenv("TOP_K", "24"))
MAX_CONTEXT_CHARS = int(os.getenv("MAX_CONTEXT_CHARS", "24000"))

DATA_DIR = Path(__file__).with_name("data")
HASHTAGS_CSV = DATA_DIR / "instagram_hashtags.csv"
GA_CSV = DATA_DIR / "ga_metrics.csv"

# ---------------------------
# Output sanitizer
# ---------------------------
_URL_RE = re.compile(r"https?://[^\s)>\]]+", re.IGNORECASE)
_MD_LINK_RE = re.compile(r"\[([^\]]+)\]\((https?://[^)]+)\)")
# add near your other routes
@app.get("/")
def home():
    return (
        """
        <html>
          <head><title>DataDepot API</title></head>
          <body style="font-family: system-ui; max-width: 700px; margin: 40px auto; line-height:1.5">
            <h1>DataDepot API is live ✅</h1>
            <p>Try these endpoints:</p>
            <ul>
              <li><code>GET /status</code></li>
              <li><code>POST /ask</code> with JSON: <code>{"question": "Hello"}</code></li>
            </ul>
          </body>
        </html>
        """,
        200,
        {"Content-Type": "text/html"},
    )


def _sanitize_answer_format(text: str, max_bullets: int = 5):
    """
    Clean up model output into neat, spaced sections:
    - Remove all URLs (inline, markdown, parenthetical domains)
    - Convert ### headings to bold
    - Normalize bullets to •
    - Ensure blank lines between headings, paragraphs, and bullet blocks
    - Limit bullets per section (default 5)
    Returns (clean_text, []) since links/sources are stripped
    """
    if not text:
        return "", []

    # 1) Remove markdown links [title](url) → keep only title
    text = _MD_LINK_RE.sub(lambda m: m.group(1).strip(), text)

    # 2) Remove raw URLs entirely
    text = _URL_RE.sub("", text)

    # 3) Remove parenthetical domains like (apnews.com)
    text = re.sub(r"\([a-z0-9\.\-]+\.com\)", "", text, flags=re.IGNORECASE)

    lines = [ln.rstrip() for ln in text.splitlines()]

    out_lines = []
    bullet_buffer = []

    def flush_bullets():
        nonlocal bullet_buffer
        if bullet_buffer:
            if len(bullet_buffer) > max_bullets:
                bullet_buffer = bullet_buffer[:max_bullets] + ["• …"]
            if out_lines and out_lines[-1] != "":
                out_lines.append("")
            out_lines.extend(bullet_buffer)
            out_lines.append("")
            bullet_buffer = []

    for ln in lines:
        if ln.startswith("###") or ln.startswith("## "):
            flush_bullets()
            if out_lines and out_lines[-1] != "":
                out_lines.append("")
            heading = ln.lstrip("# ").strip()
            out_lines.append(f"**{heading}**")
            out_lines.append("")
        elif re.match(r"^\s*-\s+", ln):
            bullet_buffer.append(re.sub(r"^\s*-\s+", "• ", ln.strip()))
        elif ln.strip():
            flush_bullets()
            if out_lines and out_lines[-1] != "":
                out_lines.append("")
            out_lines.append(ln.strip())

    flush_bullets()

    # Collapse excessive blank lines (max 2)
    clean_text = re.sub(r"\n{3,}", "\n\n", "\n".join(out_lines)).strip()

    return clean_text, []  # always return empty sources

# ---------------------------
# Hashtag fallback (CSV)
# ---------------------------
def _hashtag_fallback() -> str:
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
            bullets.append(f"• {tag} (frequency: {freq})" if freq else f"• {tag}")
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
    lower_map = {k.lower(): k for k in row.keys()}
    for n in names:
        key = lower_map.get(n.lower())
        if key:
            return key
    return ""

def _load_ga_rows():
    out = []
    if not GA_CSV.exists():
        return out
    with GA_CSV.open("r", encoding="utf-8", newline="") as f:
        rdr = csv.DictReader(f)
        if not rdr.fieldnames:
            return out
        for raw in rdr:
            if not raw:
                continue
            date_col    = _find_col(raw, ["date", "Date"])
            country_col = _find_col(raw, ["country", "Country"])
            page_col    = _find_col(raw, ["pagePath", "page", "page_title", "Page path and screen class", "pagePathPlusQuery"])
            users_col   = _find_col(raw, ["activeUsers", "users", "Users"])
            events_col  = _find_col(raw, ["eventCount", "Events"])
            dt = None
            if date_col:
                ds = (raw.get(date_col) or "").strip()
                if len(ds) == 8 and ds.isdigit():
                    try: dt = datetime.strptime(ds, "%Y%m%d").date()
                    except Exception: dt = None
                else:
                    try: dt = datetime.fromisoformat(ds).date()
                    except Exception: dt = None
            country = (raw.get(country_col) or "").strip() if country_col else None
            page    = (raw.get(page_col) or "").strip() if page_col else None
            users   = _parse_int(raw.get(users_col), 0) if users_col else 0
            events  = _parse_int(raw.get(events_col), 0) if events_col else 0
            out.append({"date": dt, "country": country or None, "page": page or None, "users": users, "events": events})
    return out

def _ga_in_window(rows, days: int):
    valid = [r for r in rows if r["date"] is not None]
    if not valid: return []
    latest = max(r["date"] for r in valid)
    start = latest - timedelta(days=days - 1)
    return [r for r in valid if start <= r["date"] <= latest]

def _ga_summary(rows, days: int):
    w = _ga_in_window(rows, days)
    total_users = sum(r["users"] for r in w)
    total_events = sum(r["events"] for r in w)
    by_day = {}
    for r in w: by_day[r["date"]] = by_day.get(r["date"], 0) + r["users"]
    day_lines = [f"• {d.isoformat()}: {by_day[d]} users" for d in sorted(by_day.keys())]
    return (
        f"**Google Analytics — last {days} days**\n\n"
        f"• Total active users: **{total_users}**\n"
        f"• Total events: **{total_events}**\n\n"
        f"**Daily users**\n" + ("\n".join(day_lines) if day_lines else "• (no daily rows)")
    )

def _ga_top_countries(rows, days: int, limit: int = 5):
    w = _ga_in_window(rows, days)
    agg = {}
    for r in w: agg[r["country"] or "(unknown)"] = agg.get(r["country"] or "(unknown)", 0) + r["users"]
    top = sorted(agg.items(), key=lambda x: x[1], reverse=True)[:limit]
    if not top: return "I don’t have GA country data in the dataset."
    lines = ["**Top countries by users**"]
    for c, n in top: lines.append(f"• {c}: {n}")
    return "\n".join(lines)

def _ga_top_pages(rows, days: int, limit: int = 5):
    w = _ga_in_window(rows, days)
    agg = {}
    for r in w: agg[r["page"] or "(unknown)"] = agg.get(r["page"] or "(unknown)", 0) + r["users"]
    top = sorted(agg.items(), key=lambda x: x[1], reverse=True)[:limit]
    if not top: return "I don’t have GA page data in the dataset."
    lines = ["**Top pages by users**"]
    for p, n in top: lines.append(f"• {p}: {n}")
    return "\n".join(lines)

def _ga_daily_users(rows, days: int):
    w = _ga_in_window(rows, days)
    by_day = {}
    for r in w: by_day[r["date"]] = by_day.get(r["date"], 0) + r["users"]
    lines = ["**Daily active users**"]
    for d in sorted(by_day.keys()): lines.append(f"• {d.isoformat()}: {by_day[d]}")
    if len(lines) == 1: return "I don’t have GA daily user data."
    return "\n".join(lines)

def _maybe_answer_ga(q_lower: str) -> str | None:
    if not GA_CSV.exists(): return None
    ga_mention = ("ga" in q_lower) or ("google analytics" in q_lower) or ("analytics" in q_lower)
    if not ga_mention and not any(kw in q_lower for kw in ["top countries","top pages","busiest","total active users","daily active users","daily users"]):
        return None
    days = 30 if "30" in q_lower or "last month" in q_lower else 7
    rows = _load_ga_rows()
    if not rows: return None
    if "top countries" in q_lower: return _ga_top_countries(rows, days)
    if "top pages" in q_lower: return _ga_top_pages(rows, days)
    if "daily active users" in q_lower or "daily users" in q_lower: return _ga_daily_users(rows, days)
    return _ga_summary(rows, days)

@app.get("/status")
def status():
    return jsonify({"ok": True})

@app.post("/ask")
def ask():
    try:
        data = request.get_json(force=True) or {}
        q = (data.get("question") or "").strip()
        if not q: return jsonify({"error": "Missing question"}), 400

        q_lower = q.lower()
        use_web = bool(data.get("web"))
        web_domains = data.get("web_domains") or []

        ga_try = _maybe_answer_ga(q_lower)
        if ga_try: return jsonify({"answer": ga_try, "sources": []})

        if "hashtag" in q_lower or q_lower.startswith("#") or "hashtags" in q_lower:
            fallback = _hashtag_fallback()
            if fallback: return jsonify({"answer": fallback, "sources": []})

        qvec = embed_text(q)
        hits = search(qvec, top_k=TOP_K)
        chunks = [h.get("payload", {}).get("text", "") for h in hits if h.get("payload")]
        context = "\n\n---\n\n".join([c for c in chunks if c])[:MAX_CONTEXT_CHARS]

        wants_fresh = any(kw in q_lower for kw in ["today","latest","this week","breaking","current","news","2025"])
        if _env_bool("ENABLE_WEB_SEARCH", False) and (use_web or wants_fresh or not context.strip()):
            from openai_integration import web_answer_updated
            wa = web_answer_updated(question=q, allowed_domains=web_domains if web_domains else None)
            clean_text, _ = _sanitize_answer_format(wa.get("text") or "")
            if clean_text.strip():
                return jsonify({"answer": clean_text, "sources": []})

        if context.strip():
            raw_ans = (chat_answer(context, q, temperature=0.2) or "").strip()
            clean_text, _ = _sanitize_answer_format(raw_ans)
            return jsonify({"answer": clean_text, "sources": []})

        return jsonify({"answer": "I don’t know from the current dataset.", "sources": []})
    except Exception as e:
        return jsonify({"error": f"{type(e).__name__}: {e}", "answer": "", "sources": []}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT, debug=True)
