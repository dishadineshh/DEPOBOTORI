# openai_integration.py
import os
import time
import random
import requests
from dotenv import load_dotenv

load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
BASE_URL = os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1")

if not OPENAI_API_KEY:
    raise RuntimeError("OPENAI_API_KEY is not set. Put it in back/.env")

_DEFAULT_HEADERS = {
    "Authorization": f"Bearer {OPENAI_API_KEY}",
    "Content-Type": "application/json",
}

def _headers():
    # Kept as a function in case you later rotate tokens at runtime
    return _DEFAULT_HEADERS

def _post_with_retry(url: str, json_payload: dict, timeout: int = 120, max_retries: int = 3):
    """Simple retry wrapper for transient 429/5xx from OpenAI."""
    backoff = 1.5
    for i in range(max_retries):
        r = requests.post(url, headers=_headers(), json=json_payload, timeout=timeout)
        # Retry on rate limit / server errors
        if r.status_code in (429, 500, 502, 503, 504):
            if i < max_retries - 1:
                time.sleep(backoff ** (i + 1))
                continue
        r.raise_for_status()
        return r

    # If we got here, last response still failed; raise
    r.raise_for_status()
    return r  # never reached, but keeps typing tools happy

def embed_text(text: str):
    """
    Get a vector embedding for the given text.
    Uses OpenAI text-embedding-3-small (1536 dims).
    """
    url = f"{BASE_URL}/embeddings"
    payload = {
        "model": "text-embedding-3-small",
        "input": text
    }
    r = _post_with_retry(url, payload, timeout=60)
    return r.json()["data"][0]["embedding"]

# A few varied, concise follow-up closers to avoid repetition
_CLOSERS = [
    "Would you like a quick example from the dataset?",
    "Want me to expand on one point?",
    "Should I list a few key takeaways?",
    "Want this summarized even shorter?",
    "Shall I compare this with another channel?"
]

def _closer():
    return random.choice(_CLOSERS)

def chat_answer(context: str, question: str, temperature: float = 0.2) -> str:
    """
    Ask the chat model for a grounded, well-formatted answer.

    Rules enforced in the system message:
      - Use ONLY the provided context. If the context is insufficient, say:
        "I don’t know from the current dataset."
      - No guessing or fabrication. Never invent examples.
      - Formatting:
          * If the user asks for bullets or the question is a list (services/themes/steps),
            respond in concise bullet points (max 5–8).
          * If the question asks for a definition/description/summary, use 2–3 clean sentences.
          * For comparisons/strategies, use short paragraphs with spacing.
      - Keep answers concise, scannable, and professional.
      - Do NOT include raw source URLs or a "Sources:" section in the text.
      - End with one short, varied conversational closer.
    """
    system = (
        "You are DataDepot, Upload Digital’s knowledge bot. "
        "Answer ONLY using the supplied context. "
        "If the context is insufficient, say: \"I don’t know from the current dataset.\" "
        "Do not guess or fabricate, and never invent examples.\n\n"
        "Formatting rules:\n"
        "- If the user asks for BULLETS or the question implies a list (services/themes/steps), "
        "  respond with concise bullet points (5–8 max), using short phrases, no long paragraphs.\n"
        "- If asked for a definition/description/summary, reply in 2–3 sentences.\n"
        "- For comparisons or strategies, use short paragraphs with clear spacing.\n"
        "- Keep it concise and scannable. Avoid filler.\n"
        "- Do NOT include raw URLs or any 'Sources:' block in the answer text."
    )

    user = (
        f"Question: {question}\n\n"
        f"Context (use this to answer; if it's not enough, say you don't know):\n{context}"
    )

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

    # Ensure we end with a short, varied closer (but don't duplicate if model already did)
    c = _closer()
    if c not in msg:
        msg = (msg + "\n\n" + c).strip()

    return msg
