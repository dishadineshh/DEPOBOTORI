# extract_instagram_hashtags.py
import csv, re, os
from collections import Counter

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
SOURCES = [
    os.path.join(DATA_DIR, "gsheets_corpus.csv"),
    os.path.join(DATA_DIR, "google_docs_corpus.csv"),
]

HASHTAG_RE = re.compile(r"#(\w+)", re.UNICODE)

def main():
    counts = Counter()
    examples = {}  # keep one example source per hashtag

    for path in SOURCES:
        if not os.path.exists(path):
            continue
        with open(path, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                text = (row.get("text") or "") + " " + (row.get("title") or "")
                source = (row.get("source") or "").strip()
                for tag in HASHTAG_RE.findall(text):
                    ht = f"#{tag}"
                    counts[ht] += 1
                    if ht not in examples:
                        examples[ht] = source

    out_path = os.path.join(DATA_DIR, "instagram_hashtags.csv")
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["text", "source"])
        writer.writeheader()
        # write each hashtag as its own short "document"
        for ht, n in counts.most_common():
            src = examples.get(ht, "")
            writer.writerow({
                "text": f"{ht} (frequency: {n})",
                "source": src
            })

    print(f"Saved: {out_path} rows: {len(counts)}")
    if not counts:
        print("No hashtags were found in the current corpus. "
              "Add hashtags to your Sheets/Docs or use the GDrive path.")
        
if __name__ == "__main__":
    main()
