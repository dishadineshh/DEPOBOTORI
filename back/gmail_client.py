# back/gmail_client.py
from __future__ import annotations
import os, json
from pathlib import Path
from typing import List, Dict
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]
BASE_DIR = Path(__file__).resolve().parent
KEYS_DIR = BASE_DIR / "keys"
CREDS_PATH = KEYS_DIR / "credentials.json"
TOKEN_PATH = KEYS_DIR / "token.json"

def _ensure_key_files_from_env():
    """Optionally hydrate credentials/token from env vars (for Render)."""
    KEYS_DIR.mkdir(parents=True, exist_ok=True)
    c_env = os.getenv("GMAIL_CREDS_JSON")
    t_env = os.getenv("GMAIL_TOKEN_JSON")
    if c_env and not CREDS_PATH.exists():
        CREDS_PATH.write_text(c_env, encoding="utf-8")
    if t_env and not TOKEN_PATH.exists():
        TOKEN_PATH.write_text(t_env, encoding="utf-8")

def get_service():
    _ensure_key_files_from_env()
    creds = None
    if TOKEN_PATH.exists():
        creds = Credentials.from_authorized_user_file(str(TOKEN_PATH), SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
            TOKEN_PATH.write_text(creds.to_json(), encoding="utf-8")
        else:
            raise RuntimeError("Gmail token.json missing/invalid on server.")
    return build("gmail", "v1", credentials=creds)

def whoami() -> str:
    svc = get_service()
    prof = svc.users().getProfile(userId="me").execute()
    return prof.get("emailAddress", "(unknown)")

def list_messages(max_results: int = 10, q: str = "") -> List[Dict]:
    svc = get_service()
    resp = svc.users().messages().list(userId="me", maxResults=max_results, q=q).execute()
    return resp.get("messages", [])

def fetch_snippets(msg_ids: List[str]) -> List[str]:
    svc = get_service()
    out = []
    for mid in msg_ids:
        m = svc.users().messages().get(userId="me", id=mid, format="metadata", metadataHeaders=["Subject","From","Date","Snippet"]).execute()
        snippet = m.get("snippet", "")
        headers = {h["name"]: h["value"] for h in m.get("payload", {}).get("headers", [])}
        subj = headers.get("Subject", "(no subject)")
        frm = headers.get("From", "(unknown)")
        dt  = headers.get("Date", "")
        out.append(f"• {subj} — {frm} — {dt}\n  {snippet}")
    return out

def quick_summary(limit=5, q=""):
    msgs = list_messages(max_results=limit, q=q)
    ids = [m["id"] for m in msgs]
    lines = fetch_snippets(ids)
    who = whoami()
    header = f"**Gmail — {who}**"
    if q:
        header += f"\n(filter: `{q}`)"
    return "\n".join([header, *lines]) if lines else header + "\n(no messages)"
