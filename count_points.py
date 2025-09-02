import os, requests
from dotenv import load_dotenv
load_dotenv()
url = os.getenv("QDRANT_URL")
key = os.getenv("QDRANT_API_KEY")
r = requests.post(
    f"{url}/collections/company_knowledge/points/count",
    headers={"api-key": key, "Content-Type":"application/json"},
    json={"exact": True},
    timeout=20,
)
print(r.status_code, r.text)
