# drop_collection.py
import os, requests
from dotenv import load_dotenv
load_dotenv()

base = os.getenv("QDRANT_URL")
key  = os.getenv("QDRANT_API_KEY")
coll = os.getenv("QDRANT_COLLECTION", "company_knowledge")

r = requests.delete(f"{base}/collections/{coll}", headers={"api-key":key}, timeout=20)
print(r.status_code, r.text)
