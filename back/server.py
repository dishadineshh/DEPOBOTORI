import os
import re
import csv
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List

from dotenv import load_dotenv
from flask import Flask, request, jsonify
from flask_cors import CORS

# Load .env (back/.env)
ENV_PATH = Path(__file__).with_name(".env")
load_dotenv(dotenv_path=ENV_PATH)

# --- Safe startup logs (never print secrets) ---
wm = os.getenv("WEB_MODEL")
wad = os.getenv("WEB_ALLOWED_DOMAINS")
qdrant_url = os.getenv("QDRANT_URL")
qdrant_key = (os.getenv("QDRANT_API_KEY") or "").strip()

print(f"[boot] WEB_MODEL set to {wm if wm else '(default)'}")
print("[boot] WEB_ALLOWED_DOMAINS configured" if wad else "[boot] WEB_ALLOWED_DOMAINS not set")
print("[boot] QDRANT_URL configured" if qdrant_url else "[boot] QDRANT_URL not set")
print(f"[boot] QDRANT_API_KEY loaded (length {len(qdrant_key)})" if qdrant_key else "[boot] QDRANT_API_KEY missing")
print("[boot] OpenAI API key loaded" if (os.getenv("OPENAI_API_KEY") or "").strip() else "[boot] OpenAI API key MISSING")

# Imports that use env
from openai_integration import embed_text, chat_answer, web_answer_updated
from qdrant_rest import search

# ---- Asana integration (optional) ----
try:
    from asana_integration import (
        asana_available,
        asana_answer,
        refresh_asana_cache,
        list_workspaces,
        list_projects,
    )
    print("[boot] Asana integration loaded")
except Exception as _e:
    # Safe fallbacks if the module isn't present/valid
    asana_available = lambda: False                          # type: ignore
    asana_answer = lambda q: "Asana integration disabled"    # type: ignore
    refresh_asana_cache = lambda force=True: []              # type: ignore
    list_workspaces = lambda: []                             # type: ignore
    list_projects = lambda ws=None: []                       # type: ignore
    print(f"[boot] Asana integration NOT loaded ({type(_e).__name__})")

# ---------------------------
# Config & helpers
# ---------------------------
def _env_bool(name: str, default: bool = False) -> bool:
    v = (os.getenv(name) or "").strip().lower()
    if v in ("1", "true", "yes", "on"):
        return True
    if v in ("0", "false", "no", "off"):
        return False
    return default

PORT = int(os.getenv("PORT", "8000"))
CORS_ORIGINS = [o.strip() for o in os.getenv("CORS_ORIGINS", "http://localhost:3000").split(",")]
ASANA_WORKSPACE_ID = (os.getenv("ASANA_WORKSPACE_ID") or "").strip()

# ✅ Feature flags
ENABLE_GA = _env_bool("ENABLE_GA", True)
ENABLE_WEB_SEARCH = _env_bool("ENABLE_WEB_SEARCH", True)

app = Flask(__name__)

# CORS – allow your Netlify site and localhost
CORS(
    app,
    resources={
        r"/*": {
            "origins": CORS_ORIGINS,
            "methods": ["GET", "POST", "OPTIONS"],
            "allow_headers": ["Content-Type", "Authorization"],
            "supports_credentials": False,
            "max_age": 86400,
        }
    },
)

TOP_K = int(os.getenv("TOP_K", "24"))
MAX_CONTEXT_CHARS = int(os.getenv("MAX_CONTEXT_CHARS", "24000"))

DATA_DIR = Path(__file__).with_name("data")
HASHTAGS_CSV = DATA_DIR / "instagram_hashtags.csv"
GA_CSV = DATA_DIR / "ga_metrics.csv"

# ---------------------------
# Secret redaction in error messages
# ---------------------------
_SECRET_PATTERNS = [
    re.compile(r"sk-[A-Za-z0-9\-_]{20,}"),
    re.compile(r"Bearer\s+[A-Za-z0-9\-_\.]{20,}"),
    re.compile(r"[A-Za-z0-9]{32,}"),
]

def _sanitize_error_message(msg: str) -> str:
    if not msg:
        return msg
    safe = msg
    for pat in _SECRET_PATTERNS:
        safe = pat.sub("[REDACTED]", safe)
    return safe

# ---------------------------
# Output sanitizer (formatting)
# ---------------------------
_URL_RE = re.compile(r"https?://[^\s)>\]]+", re.IGNORECASE)
_MD_LINK_RE = re.compile(r"\[([^\]]+)\]\((https?://[^)]+)\)")

def _sanitize_answer_format(text: str, max_bullets: int = 5):
    if not text:
        return "", []
    # remove [title](url) → keep title
    text = _MD_LINK_RE.sub(lambda m: m.group(1).strip(), text)
    # remove raw URLs
    text = _URL_RE.sub("", text)
    # remove parenthetical domains like (example.com)
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
    clean_text = re.sub(r"\n{3,}", "\n\n", "\n".join(out_lines)).strip()
    return clean_text, []

# ---------------------------
# Instagram Hashtags (CSV)
# ---------------------------
def _load_hashtags_rows():
    rows = []
    if not HASHTAGS_CSV.exists():
        return rows
    with HASHTAGS_CSV.open("r", encoding="utf-8", newline="") as f:
        rdr = csv.DictReader(f)
        for raw in rdr:
            tag = (raw.get("hashtag") or raw.get("tag") or "").strip()
            if not tag:
                continue
            if tag.startswith("#"):
                tag = tag[1:]
            freq = raw.get("freq") or raw.get("count") or raw.get("frequency") or ""
            try:
                freq_val = int(str(freq).replace(",", "").strip() or "0")
            except Exception:
                freq_val = 0
            rows.append({"hashtag": tag, "freq": freq_val})
    return rows

def _hashtags_top(n: int = 10) -> str:
    rows = _load_hashtags_rows()
    if not rows:
        return "I don’t have an Instagram hashtags CSV on this server yet."
    top = sorted(rows, key=lambda r: r["freq"], reverse=True)[: max(1, n)]
    lines = [f"**Top {len(top)} hashtags**"]
    for r in top:
        lines.append(f"• #{r['hashtag']} — {r['freq']}")
    return "\n".join(lines)

def _hashtags_trending() -> str:
    return _hashtags_top(10)

def _hashtags_suggest(query: str, limit: int = 12) -> str:
    rows = _load_hashtags_rows()
    if not rows:
        return "I don’t have an Instagram hashtags CSV on this server yet."
    q = (query or "").lower().strip()
    topic = q
    for sep in ["for ", "relevant for ", "about ", "on "]:
        if sep in q:
            topic = q.split(sep, 1)[1]
            break
    kws = [w for w in re.split(r"[^a-z0-9]+", topic) if w]
    if not kws:
        return _hashtags_top(limit)
    scored = []
    for r in rows:
        tag_l = r["hashtag"].lower()
        score = sum(1 for k in kws if k in tag_l)
        if score > 0:
            scored.append((score, r))
    if not scored:
        return _hashtags_top(limit)
    scored.sort(key=lambda x: (x[0], x[1]["freq"]), reverse=True)
    picked = [r for _, r in scored[:limit]]
    lines = [f"**Suggested hashtags (topic: {topic})**"]
    for r in picked:
        lines.append(f"• #{r['hashtag']} — {r['freq']}")
    return "\n".join(lines)

def _hashtags_any() -> str:
    rows = _load_hashtags_rows()
    if not rows:
        return "I don’t have an Instagram hashtags CSV on this server yet."
    rows = sorted(rows, key=lambda r: r["freq"], reverse=True)[:50]
    return "\n".join([f"• #{r['hashtag']} — {r['freq']}" for r in rows])

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

def _find_col(row: dict, names: List[str]) -> str:
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

            dt_val = None
            if date_col:
                ds = (raw.get(date_col) or "").strip()
                if len(ds) == 8 and ds.isdigit():
                    try:
                        dt_val = datetime.strptime(ds, "%Y%m%d").date()
                    except Exception:
                        dt_val = None
                else:
                    try:
                        dt_val = datetime.fromisoformat(ds).date()
                    except Exception:
                        dt_val = None

            country = (raw.get(country_col) or "").strip() if country_col else None
            page    = (raw.get(page_col) or "").strip() if page_col else None
            users   = _parse_int(raw.get(users_col), 0) if users_col else 0
            events  = _parse_int(raw.get(events_col), 0) if events_col else 0

            out.append({"date": dt_val, "country": country or None, "page": page or None, "users": users, "events": events})
    return out

def _ga_in_window(rows, days: int):
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
    by_day = {}
    for r in w:
        by_day[r["date"]] = by_day.get(r["date"], 0) + r["users"]
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
    for r in w:
        agg[r["country"] or "(unknown)"] = agg.get(r["country"] or "(unknown)", 0) + r["users"]
    top = sorted(agg.items(), key=lambda x: x[1], reverse=True)[:limit]
    if not top:
        return "I don’t have GA country data in the dataset."
    lines = ["**Top countries by users**"]
    for c, n in top:
        lines.append(f"• {c}: {n}")
    return "\n".join(lines)

def _ga_top_pages(rows, days: int, limit: int = 5):
    w = _ga_in_window(rows, days)
    agg = {}
    for r in w:
        agg[r["page"] or "(unknown)"] = agg.get(r["page"] or "(unknown)", 0) + r["users"]
    top = sorted(agg.items(), key=lambda x: x[1], reverse=True)[:limit]
    if not top:
        return "I don’t have GA page data in the dataset."
    lines = ["**Top pages by users**"]
    for p, n in top:
        lines.append(f"• {p}: {n}")
    return "\n".join(lines)

def _ga_daily_users(rows, days: int):
    w = _ga_in_window(rows, days)
    by_day = {}
    for r in w:
        by_day[r["date"]] = by_day.get(r["date"], 0) + r["users"]
    lines = ["**Daily active users**"]
    for d in sorted(by_day.keys()):
        lines.append(f"• {d.isoformat()}: {by_day[d]}")
    if len(lines) == 1:
        return "I don’t have GA daily user data."
    return "\n".join(lines)

# ✅ STRICT GA trigger — NEVER hijack non-GA queries
def _maybe_answer_ga(q_lower: str) -> Optional[str]:
    if not ENABLE_GA:
        return None
    # Only consider GA *if* user clearly asked about GA/Analytics
    ga_mention = (
        re.search(r"\bga\b", q_lower) is not None
        or "google analytics" in q_lower
        or re.search(r"\banalytics\b", q_lower) is not None
    )
    if not ga_mention and not any(kw in q_lower for kw in [
        "top countries", "top pages", "busiest",
        "total active users", "daily active users", "daily users"
    ]):
        return None

    # If the CSV isn't present, DO NOT hijack other routes
    if not GA_CSV.exists():
        return None

    rows = _load_ga_rows()
    if not rows:
        return None

    days = 30 if "30" in q_lower or "last month" in q_lower else 7
    if "top countries" in q_lower:
        return _ga_top_countries(rows, days)
    if "top pages" in q_lower:
        return _ga_top_pages(rows, days)
    if "daily active users" in q_lower or "daily users" in q_lower:
        return _ga_daily_users(rows, days)
    return _ga_summary(rows, days)

# ---------------------------
# Routes
# ---------------------------
@app.get("/status")
def status():
    return jsonify({
        "ok": True,
        "asana": bool(asana_available()),
        "ga": ENABLE_GA,
        "web": ENABLE_WEB_SEARCH
    })

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
              <li><code>GET /diag/web</code> — web formatter diag</li>
              <li><code>GET /diag/ga</code> — GA CSV diag</li>
              <li><code>GET /diag/hashtags</code> — Instagram hashtags CSV diag</li>
              <li><code>POST /ask</code> with JSON: <code>{"question": "Hello"}</code></li>
              <li><code>GET /asana/workspaces</code> — if ASANA_PAT is set</li>
              <li><code>GET /asana/projects?workspace=&lt;gid&gt;</code> — or set <code>ASANA_WORKSPACE_ID</code></li>
              <li><code>POST /asana/refresh</code> — refresh Asana cache</li>
            </ul>
          </body>
        </html>
        """,
        200,
        {"Content-Type": "text/html"},
    )

@app.get("/asana/workspaces")
def asana_ws():
    if not asana_available():
        return jsonify({"error": "Asana not configured"}), 400
    return jsonify({"workspaces": list_workspaces()})

@app.get("/asana/projects")
def asana_projects():
    if not asana_available():
        return jsonify({"error": "Asana not configured"}), 400
    ws = request.args.get("workspace") or ASANA_WORKSPACE_ID
    if not ws:
        return jsonify({"error": "Missing ?workspace=<gid> and ASANA_WORKSPACE_ID not set"}), 400
    return jsonify({"projects": list_projects(ws)})

@app.post("/asana/refresh")
def asana_refresh():
    if not asana_available():
        return jsonify({"error": "Asana not configured"}), 400
    projs = refresh_asana_cache(force=True)
    return jsonify({"ok": True, "projects": projs})

@app.get("/diag/ga")
def diag_ga():
    return jsonify({
        "enabled": ENABLE_GA,
        "csv_present": GA_CSV.exists(),
    })

@app.get("/diag/web")
def diag_web():
    return jsonify({
        "enabled": ENABLE_WEB_SEARCH,
        "model": wm or "(default)"
    })

@app.get("/diag/hashtags")
def diag_hashtags():
    return jsonify({
        "csv_present": HASHTAGS_CSV.exists(),
        "count": len(_load_hashtags_rows())
    })

@app.post("/ask")
def ask():
    try:
        data = request.get_json(force=True) or {}
        q = (data.get("question") or "").strip()
        if not q:
            return jsonify({"error": "Missing question"}), 400

        q_lower = q.lower()

        # 0) WEB first (explicit mode or “newsy” keywords) so GA can't preempt
        web_mode = str(data.get("mode") or "").lower() == "web"
        newsy = any(k in q_lower for k in ["news", "breaking", "today", "latest", "headline", "update"])
        if ENABLE_WEB_SEARCH and (web_mode or newsy):
            print("[/ask] path=WEB")
            allowed = None
            wad_env = os.getenv("WEB_ALLOWED_DOMAINS")
            if wad_env:
                allowed = [d.strip() for d in wad_env.split(",") if d.strip()]
            try:
                wa = web_answer_updated(question=q, allowed_domains=allowed)
                clean_text, _ = _sanitize_answer_format(wa.get("text", ""))
                if clean_text:
                    return jsonify({"answer": clean_text, "sources": wa.get("sources", [])})
            except Exception as e:
                # Return a fast, sanitized error instead of letting the request hang
                return jsonify({"error": _sanitize_error_message(str(e)), "answer": "", "sources": []}), 502

        # 1) Asana questions first (if PAT available)
        if asana_available() and any(k in q_lower for k in ["asana", "task", "ticket", "project", "tasks"]):
            print("[/ask] path=ASANA")
            ans = asana_answer(q)
            clean_text, _ = _sanitize_answer_format(ans)
            return jsonify({"answer": clean_text, "sources": []})

        # 2) GA (strict trigger, feature-flagged)
        ga_try = _maybe_answer_ga(q_lower)
        if ga_try is not None:
            print("[/ask] path=GA")
            return jsonify({"answer": ga_try, "sources": []})

        # 3) Instagram Hashtags (CSV)
        if "hashtag" in q_lower or q_lower.startswith("#") or "hashtags" in q_lower:
            print("[/ask] path=HASHTAGS")
            if "top" in q_lower:
                return jsonify({"answer": _hashtags_top(), "sources": []})
            if "trending" in q_lower:
                return jsonify({"answer": _hashtags_trending(), "sources": []})
            if "suggest" in q_lower or "relevant" in q_lower or "for " in q_lower:
                return jsonify({"answer": _hashtags_suggest(q), "sources": []})
            return jsonify({"answer": _hashtags_any(), "sources": []})

        # 4) RAG search in Qdrant
        print("[/ask] path=RAG")
        qvec = embed_text(q)
        try:
            hits = search(qvec, top_k=TOP_K)
        except Exception:
            hits = []
        chunks = [h.get("payload", {}).get("text", "") for h in hits if h.get("payload")]
        context = "\n\n---\n\n".join([c for c in chunks if c])[:MAX_CONTEXT_CHARS]

        # 5) Grounded answer using context
        if context.strip():
            try:
                raw_ans = (chat_answer(context, q, temperature=0.2) or "").strip()
                clean_text, _ = _sanitize_answer_format(raw_ans)
                return jsonify({"answer": clean_text, "sources": []})
            except Exception as e:
                return jsonify({"error": _sanitize_error_message(str(e)), "answer": "", "sources": []}), 502

        # 6) Nothing found
        return jsonify({"answer": "I don’t know from the current dataset.", "sources": []})
    except Exception as e:
        safe_msg = _sanitize_error_message(str(e))
        # Return a generic, sanitized error to the client
        return jsonify({"error": f"{type(e).__name__}: {safe_msg}", "answer": "", "sources": []}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT, debug=True)
