# ingest_hashtags.py
import os, csv, uuid
from openai_integration import embed_text
from qdrant_rest import ensure_collection, upsert_points

DATA_FILE = os.path.join(os.path.dirname(__file__), "data", "instagram_hashtags.csv")

def main():
    ensure_collection()
    if not os.path.exists(DATA_FILE):
        print(f"Missing {DATA_FILE}. Run extract_instagram_hashtags.py first.")
        return

    points = []
    with open(DATA_FILE, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            text = (row.get("text") or "").strip()
            source = (row.get("source") or "").strip()
            if not text:
                continue
            vec = embed_text(text)
            points.append({
                "id": str(uuid.uuid4()),
                "vector": vec,
                "payload": {"text": text, "source": source}
            })

    # batch upserts to avoid large payloads
    BATCH = 100
    for i in range(0, len(points), BATCH):
        upsert_points(points[i:i+BATCH])

    print(f"Ingested hashtags: {len(points)}")

if __name__ == "__main__":
    main()
