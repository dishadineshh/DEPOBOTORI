import os, requests
from dotenv import load_dotenv

load_dotenv()
key = os.getenv("OPENAI_API_KEY")
print("Using key prefix:", (key or "")[:12])

r = requests.post(
    "https://api.openai.com/v1/embeddings",
    headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
    json={"model":"text-embedding-3-small", "input":"hello world"},
    timeout=30
)

print("Status:", r.status_code)
print("Body:", r.text)
