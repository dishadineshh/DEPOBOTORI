# ingest_to_qdrant.py
import csv, os, uuid
from pathlib import Path
from dotenv import load_dotenv
from openai_integration import embed_text
from qdrant_rest import ensure_collection, upsert_points

load_dotenv()
DATA_DIR = Path(__file__).with_name("data")

# Files we maintain
FILES = [
    ("gsheets_corpus.csv", "sheet"),           # LinkedIn + Instagram rows from your 2 Sheets
    ("google_docs_corpus.csv", "gdoc"),        # Website + Newsletter docs
    ("instagram_hashtags.csv", "instagram_hashtags"),  # extracted hashtags (optional)
    # ("linkedin_posts_corpus.csv", "linkedin"),  # LinkedIn posts (optional)
    ("notion_export_corpus.csv", "notion"),    # Notion pages (optional)
]

BATCH = 64

def _rows_from_csv(path: Path, platform: str):
    if not path.exists():
        return
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            text = (row.get("text") or row.get("content") or "").strip()
            if not text:
                continue
            title = (row.get("title") or row.get("doc_title") or row.get("sheet") or "").strip()
            source = (row.get("source") or row.get("url") or row.get("link") or "").strip()

            yield {
                "text": text,
                "title": title,
                "source": source,
                "platform": platform,
            }

def _chunk(text: str, size=1000, overlap=150):
    text = " ".join(text.split())
    i = 0
    while i < len(text):
        yield text[i:i+size]
        i += size - overlap

def main():
    ensure_collection()
    pending = []
    total = 0

    for fname, platform in FILES:
        path = DATA_DIR / fname
        for row in _rows_from_csv(path, platform):
            base_meta = {
                "title": row["title"],
                "source": row["source"],
                "platform": platform,
            }
            # chunk each row so retrieval has smaller, relevant snippets
            for piece in _chunk(row["text"], size=1100, overlap=150):
                vec = embed_text(piece)
                point = {
                    "id": str(uuid.uuid4()),
                    "vector": vec,
                    "payload": {**base_meta, "text": piece},
                }
                pending.append(point)
                if len(pending) >= BATCH:
                    upsert_points(pending)
                    total += len(pending)
                    print(f"[ingest] upserted: {total}")
                    pending = []

    if pending:
        upsert_points(pending)
        total += len(pending)
        print(f"[ingest] upserted: {total}")

    print("Done.")

if __name__ == "__main__":
    main()
