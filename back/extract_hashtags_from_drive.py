# extract_hashtags_from_drive.py
import csv, re, sys
from pathlib import Path

# ----------------------------
# Config
# ----------------------------
DATA_DIR = Path(__file__).with_name("data")
SRC = DATA_DIR / "drive_export_corpus.csv"
OUT = DATA_DIR / "instagram_hashtags.csv"

# Optional filter: only tally hashtags from certain subfolders in source paths.
# Leave empty [] to include everything.
INCLUDE_SOURCE_SUBSTRINGS = [
    r"\Upload\Upload Instagram and LinkedIn\Instagram\\",  # adjust to your actual folder names
]

# Lift CSV field-size limit (Drive export rows can be huge)
try:
    csv.field_size_limit(sys.maxsize)
except OverflowError:
    csv.field_size_limit(2**31 - 1)

# Regex to match hashtags
TAG_RE = re.compile(r"(#[A-Za-z0-9_]+)")
counts: dict[str, int] = {}

def _include_source(src: str) -> bool:
    """Check if a file path should be included based on INCLUDE_SOURCE_SUBSTRINGS."""
    if not INCLUDE_SOURCE_SUBSTRINGS:
        return True
    s = src.lower()
    return any(sub.lower() in s for sub in INCLUDE_SOURCE_SUBSTRINGS)

def _tally(text: str):
    if not text:
        return
    for tag in TAG_RE.findall(text):
        t = tag.strip()
        if not t:
            continue
        # normalize case so #UploadDigital == #uploaddigital
        counts[t.lower()] = counts.get(t.lower(), 0) + 1

def _read_with_csv_dictreader(path: Path) -> bool:
    """Try to parse with DictReader (preferred)."""
    try:
        with path.open("r", encoding="utf-8", newline="") as f:
            rdr = csv.DictReader(f)
            if not rdr.fieldnames:
                return False
            # Find likely text and source columns
            lower_fields = [h.lower() for h in rdr.fieldnames]
            for row in rdr:
                src = row.get("source") or row.get("Source") or ""
                txt = (
                    row.get("text")
                    or row.get("Text")
                    or row.get("TEXT")
                    or ""
                )
                if _include_source(src):
                    _tally(txt)
        return True
    except csv.Error:
        return False

def _read_with_manual_fallback(path: Path):
    """Fallback: simple CSV parsing when DictReader fails."""
    with path.open("r", encoding="utf-8", newline="") as f:
        header = f.readline()  # skip header line
        for line in f:
            line = line.rstrip("\n")
            if not line:
                continue
            parts = line.split(",", 1)  # split only once
            src = parts[0] if len(parts) > 0 else ""
            text = parts[1] if len(parts) > 1 else ""
            if _include_source(src):
                _tally(text)

def main():
    if not SRC.exists():
        print(f"[hashtags] missing {SRC}; nothing to do")
        return

    ok = _read_with_csv_dictreader(SRC)
    if not ok:
        print("[hashtags] csv.DictReader failed, using manual fallback")
        _read_with_manual_fallback(SRC)

    rows = sorted(counts.items(), key=lambda x: x[1], reverse=True)

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["hashtag", "freq"])
        w.writeheader()
        for tag, n in rows:
            w.writerow({"hashtag": tag, "freq": n})

    print(f"[hashtags] wrote {OUT} rows={len(rows)}")
    if rows[:10]:
        print("[hashtags] top 10 preview:")
        for tag, n in rows[:10]:
            print(f"  {tag} : {n}")

if __name__ == "__main__":
    main()
