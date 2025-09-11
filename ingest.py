import csv
import os 
from qdrant_client.models import PointStruct
from server import embed_text
from qdrant_client import QdrantClient


client = QdrantClient(
    url=os.getenv("QDRANT_URL"),
    api_key=os.getenv("QDRANT_API_KEY"),
)


def ingest_sheets_to_qdrant():
    COLLECTION = os.getenv("QDRANT_COLLECTION", "company_knowledge")
    with open("data/gsheets_corpus.csv", "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        points = []
        idx = 1
        for row in reader:
            text = row["text"]
            vec = embed_text(text)
            points.append(PointStruct(
                id=idx,
                vector=vec,
                payload={"text": text, "source": row["source"]}
            ))
            idx += 1
    client.upsert(collection_name=COLLECTION, points=points)
    print(f"Ingested {idx-1} rows into Qdrant.")


if __name__ == "__main__":
    # COLLECTION = os.getenv("QDRANT_COLLECTION", "company_knowledge")
    # VECTOR_SIZE = int(os.getenv("QDRANT_VECTOR_SIZE", "1536"))
    ingest_sheets_to_qdrant()