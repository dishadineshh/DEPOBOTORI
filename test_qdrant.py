import os, requests
from dotenv import load_dotenv

load_dotenv()
url = os.getenv("QDRANT_URL")
key = os.getenv("QDRANT_API_KEY")

print("QDRANT_URL =", url)
try:
    r = requests.get(
        f"{url}/collections",
        headers={"api-key": key, "Content-Type": "application/json"},
        timeout=20
    )
    print("Status:", r.status_code)
    print("Sample:", r.text[:200])
except Exception as e:
    print("Error:", e)
