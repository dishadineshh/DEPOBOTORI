# qdrant_rest.py
import os, json, uuid, requests
from dotenv import load_dotenv
from pathlib import Path

ENV_PATH = Path(__file__).with_name(".env")
load_dotenv(dotenv_path=ENV_PATH)

QDRANT_URL = os.getenv("QDRANT_URL", "").rstrip("/")
COLLECTION = os.getenv("QDRANT_COLLECTION", "company_knowledge")
VECTOR_SIZE = int(os.getenv("QDRANT_VECTOR_SIZE", "1536"))

def _headers():
    key = os.getenv("QDRANT_API_KEY", "")
    if not key:
        print("[qdrant] WARNING: QDRANT_API_KEY is EMPTY at runtime")
    return {"api-key": key, "Content-Type": "application/json"}

def ensure_collection():
    if not QDRANT_URL or not COLLECTION:
        raise RuntimeError("QDRANT_URL or QDRANT_COLLECTION not set")
    r = requests.get(f"{QDRANT_URL}/collections/{COLLECTION}", headers=_headers(), timeout=20)
    if r.status_code == 200:
        return True
    payload = {"vectors": {"size": VECTOR_SIZE, "distance": "Cosine"}}
    r = requests.put(f"{QDRANT_URL}/collections/{COLLECTION}",
                     headers=_headers(), data=json.dumps(payload), timeout=30)
    r.raise_for_status()
    return True

def _valid_uuid(s: str) -> bool:
    try:
        uuid.UUID(str(s))
        return True
    except Exception:
        return False

def _coerce_uuid(point_id):
    """
    Qdrant accepts unsigned int or UUID.
    We force UUIDs. If not valid, replace with fresh UUID4.
    """
    s = str(point_id)
    if _valid_uuid(s):
        return s
    return str(uuid.uuid4())

def upsert_points(points):
    if not points:
        return {"result": {"upserted": 0}}

    # Coerce all ids to UUID strings
    clean = []
    for p in points:
        pid = _coerce_uuid(p.get("id"))
        clean.append({
            "id": pid,
            "vector": p["vector"],
            "payload": p.get("payload", {}),
        })

    body = {"points": clean}
    r = requests.put(f"{QDRANT_URL}/collections/{COLLECTION}/points",
                     headers=_headers(), data=json.dumps(body), timeout=60)
    if r.status_code >= 400:
        print("[qdrant] UPSERT ERROR:", r.text)
    r.raise_for_status()
    return r.json()

def search(vector, top_k=5):
    if not isinstance(vector, (list, tuple)):
        raise ValueError("vector must be list/tuple of floats")
    body = {"vector": vector, "limit": int(top_k), "with_payload": True}
    r = requests.post(f"{QDRANT_URL}/collections/{COLLECTION}/points/search",
                      headers=_headers(), data=json.dumps(body), timeout=30)
    if r.status_code == 403:
        print("[qdrant] SEARCH FORBIDDEN. Check QDRANT_API_KEY and cluster URL in back/.env")
    r.raise_for_status()
    return r.json().get("result", [])

def show_collection():
    r = requests.get(f"{QDRANT_URL}/collections/{COLLECTION}", headers=_headers(), timeout=20)
    if r.status_code == 200:
        return r.json()
    return None

def drop_collection():
    requests.delete(f"{QDRANT_URL}/collections/{COLLECTION}", headers=_headers(), timeout=20)
