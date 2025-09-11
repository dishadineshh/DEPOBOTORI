# openai_integration.py
import os
import time
import random
from typing import Any, Dict, List, Optional
from openai import OpenAI
import requests
from dotenv import load_dotenv

load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    raise RuntimeError("OPENAI_API_KEY is not set. Put it in back/.env")

_client = OpenAI(api_key=OPENAI_API_KEY)

def _env_bool(name: str, default: bool = False) -> bool:
    v = (os.getenv(name) or "").strip().lower()
    if v in ("1", "true", "yes", "on"):
        return True
    if v in ("0", "false", "no", "off"):
        return False
    return default

def _parse_domains(s: str) -> List[str]:
    return [d.strip() for d in (s or "").split(",") if d.strip()]

def _gather_urls_from_response_dict(d: Any) -> List[str]:
    """Walk the response dict to collect any cited/source URLs."""
    urls = []
    if isinstance(d, dict):
        for k, v in d.items():
            if k == "url" and isinstance(v, str) and v.startswith("http"):
                urls.append(v)
            else:
                urls.extend(_gather_urls_from_response_dict(v))
    elif isinstance(d, list):
        for item in d:
            urls.extend(_gather_urls_from_response_dict(item))
    return urls

def web_answer_updated(question: str, allowed_domains: list[str] | None = None) -> dict:
    """
    Web search with enforced brief-style formatting (no links in body).
    The server sanitizes again, but we nudge the model too.
    """
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    model = os.getenv("WEB_MODEL", "gpt-4.1-mini")

    # Domain scoping hint (kept in-text for broad model compatibility)
    scoped_q = question
    if allowed_domains:
        domain_clause = " OR ".join([f"site:{d}" for d in allowed_domains])
        scoped_q = f"{question}\n\nSearch constraint: ({domain_clause})"

    formatting_rules = (
        "You are a crisp news/analysis formatter. Use the web_search tool to gather facts, "
        "then respond in clean markdown with this structure:\n\n"
        "### Title\n"
        "- one concise headline-like line\n\n"
        "### Key points\n"
        "- 3–6 punchy bullets capturing the main takeaways\n\n"
        "### What changed & why\n"
        "One or two compact paragraphs explaining drivers and implications. Avoid fluff.\n\n"
        "### What to watch next (optional)\n"
        "- 2–4 bullets on risks, next data releases, or policy decisions (include only if useful)\n\n"
        "Important rules:\n"
        "- Do NOT include any URLs, raw links, or bracketed citations in the body. "
        "Return plain text only; the client will print links separately.\n"
        "- Keep numbers/dates specific (e.g., 'August 2025', '0.1% m/m').\n"
        "- Prefer neutral, non-sensational tone."
    )

    resp = client.responses.create(
        model=model,
        tools=[{"type": "web_search"}],
        instructions=formatting_rules,
        input=[{
            "role": "user",
            "content": [{"type": "input_text", "text": scoped_q}]
        }],
        include=["web_search_call.action.sources"],
    )

    # Extract text + sources
    out_text = ""
    sources: List[str] = []
    for item in getattr(resp, "output", []) or []:
        if getattr(item, "type", None) == "message":
            for block in getattr(item, "content", []) or []:
                if getattr(block, "type", None) == "output_text":
                    out_text += block.text
        if getattr(item, "citations", None):
            for c in item.citations:
                if getattr(c, "url", None):
                    sources.append(c.url)

    # Fallback (older SDKs): parse from dict to collect any source URLs
    if not sources:
        try:
            resp_dict = resp.to_dict() if hasattr(resp, "to_dict") else resp
        except Exception:
            resp_dict = getattr(resp, "model_dump", lambda: {})()
        sources = list(dict.fromkeys(_gather_urls_from_response_dict(resp_dict)))

    return {"text": (out_text or "").strip(), "sources": list(dict.fromkeys(sources))}

def web_answer(
    question: str,
    allowed_domains: Optional[List[str]] = None,
    context_size: Optional[str] = None,  # "low" | "medium" | "high"
    location: Optional[Dict[str, str]] = None,
    model: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Legacy helper (kept for compatibility). Uses Responses API with web_search and returns:
      { "text": <answer string>, "sources": [<urls>] }
    """
    if not _env_bool("ENABLE_WEB_SEARCH", True):
        return {"text": "Web search is disabled.", "sources": []}

    model = model or (os.getenv("WEB_MODEL") or "gpt-4.1-mini")
    context_size = context_size or (os.getenv("WEB_CONTEXT_SIZE") or "medium")
    allowed_domains = allowed_domains or _parse_domains(os.getenv("WEB_ALLOWED_DOMAINS", ""))

    tool_cfg: Dict[str, Any] = {"type": "web_search"}
    if allowed_domains:
        tool_cfg["filters"] = {"allowed_domains": allowed_domains}

    loc = location or {}
    if not loc:
        country = (os.getenv("WEB_LOCATION_COUNTRY") or "").strip()
        city = (os.getenv("WEB_LOCATION_CITY") or "").strip()
        region = (os.getenv("WEB_LOCATION_REGION") or "").strip()
        if country or city or region:
            tool_cfg["user_location"] = {
                "type": "approximate",
                **({"country": country} if country else {}),
                **({"city": city} if city else {}),
                **({"region": region} if region else {}),
            }

    tool_cfg["search_context_size"] = context_size

    resp = _client.responses.create(
        model=model,
        tools=[tool_cfg],
        tool_choice="auto",
        include=["web_search_call.action.sources"],
        input=question,
    )

    text = resp.output_text if hasattr(resp, "output_text") else ""

    try:
        resp_dict = resp.to_dict() if hasattr(resp, "to_dict") else resp
    except Exception:
        resp_dict = getattr(resp, "model_dump", lambda: {})()

    urls = list(dict.fromkeys(_gather_urls_from_response_dict(resp_dict)))
    return {"text": text.strip(), "sources": urls}

BASE_URL = os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1")

_DEFAULT_HEADERS = {
    "Authorization": f"Bearer {OPENAI_API_KEY}",
    "Content-Type": "application/json",
}

def _headers():
    return _DEFAULT_HEADERS

def _post_with_retry(url: str, json_payload: dict, timeout: int = 120, max_retries: int = 3):
    backoff = 1.5
    for i in range(max_retries):
        r = requests.post(url, headers=_headers(), json=json_payload, timeout=timeout)
        if r.status_code in (429, 500, 502, 503, 504):
            if i < max_retries - 1:
                time.sleep(backoff ** (i + 1))
                continue
        r.raise_for_status()
        return r
    r.raise_for_status()
    return r

def embed_text(text: str):
    """Get a vector embedding for the given text (text-embedding-3-small, 1536 dims)."""
    url = f"{BASE_URL}/embeddings"
    payload = {"model": "text-embedding-3-small", "input": text}
    r = _post_with_retry(url, payload, timeout=60)
    return r.json()["data"][0]["embedding"]

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

    url = f"{BASE_URL}/chat/completions"
    payload = {
        "model": "gpt-4o-mini",
        "temperature": temperature,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
    }

    r = _post_with_retry(url, payload, timeout=120)
    msg = (r.json()["choices"][0]["message"]["content"] or "").strip()

    c = _closer()
    if c not in msg:
        msg = (msg + "\n\n" + c).strip()

    return msg
