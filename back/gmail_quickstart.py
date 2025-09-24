# back/gmail_quickstart.py

from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ------------------------------------------------------------
# OAuth scope: read-only Gmail access
# ------------------------------------------------------------
SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]

# ------------------------------------------------------------
# Paths (credentials.json must be in back/keys/)
# token.json will be created on first login
# ------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
KEYS_DIR = BASE_DIR / "keys"
CREDS_PATH = KEYS_DIR / "credentials.json"
TOKEN_PATH = KEYS_DIR / "token.json"


def get_creds() -> Credentials:
    """
    Load or refresh OAuth credentials, or trigger interactive login if missing.
    Uses port=0 so Google picks a free local port (avoids Windows port issues).
    """
    if not KEYS_DIR.exists():
        KEYS_DIR.mkdir(parents=True, exist_ok=True)

    creds: Optional[Credentials] = None

    # Try existing token
    if TOKEN_PATH.exists():
        creds = Credentials.from_authorized_user_file(str(TOKEN_PATH), SCOPES)

    # Refresh or run flow
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not CREDS_PATH.exists():
                raise FileNotFoundError(
                    f"Missing {CREDS_PATH}. Please put your downloaded OAuth client "
                    "as credentials.json inside back/keys/."
                )
            flow = InstalledAppFlow.from_client_secrets_file(str(CREDS_PATH), SCOPES)
            try:
                creds = flow.run_local_server(port=0, open_browser=True)
            except OSError:
                print("⚠ Local server blocked. Falling back to console copy-paste flow.")
                creds = flow.run_console()

        # Save token for next time
        TOKEN_PATH.write_text(creds.to_json(), encoding="utf-8")

    return creds


def main() -> None:
    """
    Sanity check: authenticate Gmail, show user email + 5 recent message IDs.
    """
    try:
        creds = get_creds()
        service = build("gmail", "v1", credentials=creds)

        # Who am I?
        profile = service.users().getProfile(userId="me").execute()
        email_address = profile.get("emailAddress", "(unknown)")

        # List 5 most recent messages
        resp = service.users().messages().list(userId="me", maxResults=5).execute()
        messages = resp.get("messages", [])

        print(json.dumps(
            {
                "ok": True,
                "email": email_address,
                "messages_sample": [m.get("id") for m in messages],
                "token_path": str(TOKEN_PATH),
            },
            indent=2
        ))

    except HttpError as err:
        print(json.dumps({"ok": False, "error": f"HttpError: {err}"}, indent=2))
    except Exception as e:
        print(json.dumps({"ok": False, "error": str(e)}, indent=2))


if __name__ == "__main__":
    main()
