# fetch_notion_export.py
import os
import csv
import sys
from pathlib import Path

from bs4 import BeautifulSoup

# ---------- Config ----------
# Prefer .env override; else default to your path
NOTION_EXPORT_DIR = os.getenv("NOTION_EXPORT_DIR", r"C:\Users\Admin\Desktop\notionexport")
EXPORT_DIR = Path(NOTION_EXPORT_DIR)
OUT = Path(__file__).with_name("data") / "notion_export_corpus.csv"

# Which file types to ingest
PATTERNS = ["*.html", "*.htm", "*.md", "*.txt", "*.csv"]

# ---------- Helpers ----------
def _read_text(path: Path) -> str:
    """
    Read a text file with best-effort encoding handling.
    """
    # Try utf-8 first
    try:
        return path.read_text(encoding="utf-8", errors="strict")
    except UnicodeDecodeError:
        # BOM or mixed encodings
        try:
            return path.read_text(encoding="utf-8-sig", errors="strict")
        except UnicodeDecodeError:
            # Last resort: ignore errors
            return path.read_text(encoding="utf-8", errors="ignore")

def _html_to_text(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")
    for bad in soup(["script", "style", "noscript"]):
        bad.extract()
    return soup.get_text(separator=" ", strip=True)

def _csv_to_text(path: Path) -> str:
    """
    Flatten a CSV into a single text string like `Header: value | Header: value ...`
    """
    try:
        import csv
        lines = []
        with path.open("r", encoding="utf-8", newline="") as f:
            rdr = csv.reader(f)
            rows = list(rdr)
            if not rows:
                return ""
            headers = rows[0]
            for r in rows[1:]:
                parts = []
                for h, v in zip(headers, r):
                    v = (v or "").strip()
                    if v:
                        parts.append(f"{h.strip()}: {v}")
                if parts:
                    lines.append(" | ".join(parts))
        return "\n".join(lines)
    except Exception:
        # Fallback: raw text read
        return _read_text(path)

def extract_text(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix in {".html", ".htm"}:
        return _html_to_text(_read_text(path))
    if suffix == ".md":
        return _read_text(path)
    if suffix == ".txt":
        return _read_text(path)
    if suffix == ".csv":
        return _csv_to_text(path)
    return ""  # ignore anything else

# ---------- Main ----------
def main():
    if not EXPORT_DIR.exists():
        print(f"[notion] Export folder not found: {EXPORT_DIR}")
        print("Hint: Set NOTION_EXPORT_DIR in .env or update the path in this script.")
        sys.exit(1)

    # Gather files
    files = []
    for pat in PATTERNS:
        files.extend(EXPORT_DIR.rglob(pat))

    # Debug logging to ensure we’re seeing files
    counts = {}
    for p in files:
        counts[p.suffix.lower()] = counts.get(p.suffix.lower(), 0) + 1

    print(f"[notion] scanning in: {EXPORT_DIR}")
    if not files:
        print("[notion] No matching files found. Check the folder and file extensions.")
        print("Tip: open the folder and verify you see .html/.md/.txt/.csv files.")
        # Still write an empty CSV so pipeline doesn’t break
    else:
        for ext, n in sorted(counts.items(), key=lambda x: x[0]):
            print(f"[notion] found {n} * {ext}")
        # Show a couple of examples
        for sample in files[:3]:
            print(f"[notion] sample: {sample}")

    rows = []
    for fp in files:
        try:
            text = extract_text(fp)
            if text.strip():
                rows.append({"source": str(fp), "text": text})
        except Exception as e:
            print(f"[notion] Error reading {fp}: {e}")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["source", "text"])
        w.writeheader()
        for r in rows:
            w.writerow(r)

    print(f"[notion] Saved: {OUT} rows={len(rows)}")

if __name__ == "__main__":
    main()
