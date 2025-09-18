import os
import time
import random
from typing import Any, Dict, List, Optional

import requests
from dotenv import load_dotenv

# ---------------------------------------------------------------------
# Env & constants
# ---------------------------------------------------------------------
load_dotenv()

OPENAI_API_KEY = (os.getenv("OPENAI_API_KEY") or "").strip()
if not OPENAI_API_KEY:
    raise RuntimeError("OPENAI_API_KEY is not set. Put it in back/.env")

BASE_URL = os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1")

WEB_MODEL = os.getenv("WEB_MODEL", "gpt-4o-mini")  # chat completions
CHAT_FALLBACK_MODEL = os.getenv("CHAT_FALLBACK_MODEL", WEB_MODEL)
EMBED_MODEL = os.getenv("EMBED_MODEL", "text-embedding-3-small")  # 1536 dims

_DEFAULT_HEADERS = {
    "Authorization": f"Bearer {OPENAI_API_KEY}",
    "Content-Type": "application/json",
}

# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
def _env_bool(name: str, default: bool = False) -> bool:
    v = (os.getenv(name) or "").strip().lower()
    if v in ("1", "true", "yes", "on"):
        return True
    if v in ("0", "false", "no", "off"):
        return False
    return default

def _headers():
    return _DEFAULT_HEADERS

# ✅ tighter retries & timeouts to avoid gunicorn 502s
def _post_with_retry(url: str, json_payload: dict, timeout: int = 25, max_retries: int = 2):
    backoff = 1.5
    last = None
    for i in range(max_retries):
        r = requests.post(url, headers=_headers(), json=json_payload, timeout=timeout)
        if r.status_code in (429, 500, 502, 503, 504):
            last = r
            if i < max_retries - 1:
                time.sleep(backoff ** (i + 1))
                continue
        r.raise_for_status()
        return r
    if last is not None:
        last.raise_for_status()
    raise RuntimeError("Request failed after retries")

# ---------------------------------------------------------------------
# Embeddings (REST)
# ---------------------------------------------------------------------
def embed_text(text: str):
    """Get a vector embedding for the given text (text-embedding-3-small, 1536 dims)."""
    url = f"{BASE_URL}/embeddings"
    payload = {"model": EMBED_MODEL, "input": text}
    r = _post_with_retry(url, payload, timeout=20)
    return r.json()["data"][0]["embedding"]

# ---------------------------------------------------------------------
# Chat core (REST)
# ---------------------------------------------------------------------
def _chat_complete(
    messages: List[Dict[str, str]],
    model: Optional[str] = None,
    temperature: float = 0.2,
    timeout: int = 25,  # tight to keep requests snappy
) -> str:
    url = f"{BASE_URL}/chat/completions"
    payload = {
        "model": model or CHAT_FALLBACK_MODEL,
        "temperature": temperature,
        "messages": messages,
    }
    r = _post_with_retry(url, payload, timeout=timeout)
    data = r.json()
    return (data["choices"][0]["message"]["content"] or "").strip()

# ---------------------------------------------------------------------
# Grounded chat (uses only provided context)
# ---------------------------------------------------------------------
_CLOSERS = [
    "Would you like a quick example from the dataset?",
    "Want me to expand on one point?",
    "Should I list a few key takeaways?",
    "Want this summarized even shorter?",
    "Shall I compare this with another channel?",
]
def _closer():
    return random.choice(_CLOSERS)

def chat_answer(context: str, question: str, temperature: float = 0.2) -> str:
    """Grounded answer using only supplied context."""
    system = (
        "You are DataDepot, Upload Digital’s knowledge bot. "
        "Answer ONLY using the supplied context. "
        "If the context is insufficient, say: \"I don’t know from the current dataset.\" "
        "Do not guess or fabricate, and never invent examples.\n\n"
        "Formatting rules:\n"
        "- If the user asks for BULLETS or the question implies a list (services/themes/steps), "
        "  respond with concise bullet points (5–8 max).\n"
        "- If asked for a definition/description/summary, reply in 2–3 sentences.\n"
        "- For comparisons or strategies, use short paragraphs.\n"
        "- Keep it concise; no raw URLs or a 'Sources:' block."
    )
    user = f"Question: {question}\n\nContext (use this to answer; if it's not enough, say you don't know):\n{context}"

    msg = _chat_complete(
        messages=[{"role": "system", "content": system}, {"role": "user", "content": user}],
        model=CHAT_FALLBACK_MODEL,
        temperature=temperature,
    )

    c = _closer()
    if c not in msg:
        msg = (msg + "\n\n" + c).strip()
    return msg

# ---------------------------------------------------------------------
# Web-style formatting (no live web search)
# ---------------------------------------------------------------------
def _web_style_prompt(question: str, allowed_domains: Optional[List[str]]) -> str:
    domain_hint = ""
    if allowed_domains:
        domain_clause = " OR ".join([f"site:{d}" for d in allowed_domains])
        domain_hint = f"\n\n(When relevant, consider sources like: {domain_clause})"

    formatting_rules = (
        "Format the answer as clean markdown with:\n"
        "### Title\n"
        "- one concise headline-like line\n\n"
        "### Key points\n"
        "- 3–6 punchy bullets capturing the main takeaways\n\n"
        "### What changed & why\n"
        "One or two compact paragraphs explaining drivers and implications.\n\n"
        "### What to watch next (optional)\n"
        "- 2–4 bullets on risks, next data releases, or policy decisions (only if useful)\n\n"
        "Important rules:\n"
        "- Do NOT include any URLs, raw links, or bracketed citations in the body.\n"
        "- Keep numbers/dates specific.\n"
        "- Neutral, non-sensational tone.\n"
    )
    return f"{formatting_rules}\nUser question:\n{question}{domain_hint}"

def web_answer_updated(question: str, allowed_domains: Optional[List[str]] = None) -> Dict[str, Any]:
    """Clean, link-free answer via Chat Completions (formatter only)."""
    system = "You are a crisp news/analysis formatter. Return ONLY the formatted answer. No links."
    prompt = _web_style_prompt(question, allowed_domains)
    text = _chat_complete(
        messages=[{"role": "system", "content": system}, {"role": "user", "content": prompt}],
        model=WEB_MODEL,
        temperature=0.2,
        timeout=20,  # extra tight for this path
    )
    return {"text": (text or "").strip(), "sources": []}
