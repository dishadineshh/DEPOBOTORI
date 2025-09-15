import os, re, csv
from urllib.parse import urljoin, urldefrag
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()
MAX_PAGES = int(os.getenv("MAX_PAGES","0"))
SEEDS = [s.strip() for s in os.getenv("SITE_SEEDS","").split(",") if s.strip()]
SITE_CHAR_LIMIT = int(os.getenv("SITE_CHAR_LIMIT","0")) or None

def clean_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script","style","noscript"]): tag.decompose()
    text = soup.get_text(separator=" ", strip=True)
    text = re.sub(r"\s+"," ", text).strip()
    return text[:SITE_CHAR_LIMIT] if SITE_CHAR_LIMIT else text

def crawl(seed: str, limit: int):
    origin = requests.utils.urlparse(seed).netloc
    q, seen, rows = [seed], set(), []
    while q and len(rows) < limit:
        url = q.pop(0)
        if url in seen: continue
        seen.add(url)
        try:
            r = requests.get(url, timeout=15, headers={"User-Agent":"datadepot-bot/1.0"})
            if r.status_code != 200: continue
            txt = clean_text(r.text)
            if len(txt) > 200:
                rows.append({"source":url, "text":txt})
                print("Indexed", url, f"(len={len(txt)})")
            soup = BeautifulSoup(r.text,"html.parser")
            for a in soup.find_all("a", href=True):
                u = urljoin(url, a["href"]); u = urldefrag(u)[0]
                p = requests.utils.urlparse(u)
                if p.netloc == origin and p.scheme in ("http","https") and u not in seen and len(q)<limit*3:
                    q.append(u)
        except Exception as e:
            print("skip", url, e)
    return rows

def main():
    all_rows = []
    for s in SEEDS:
        all_rows += crawl(s, MAX_PAGES)
    os.makedirs("data", exist_ok=True)
    out = os.path.join("data","uploaddigital_corpus.csv")
    with open(out,"w",newline="",encoding="utf-8") as f:
        w=csv.DictWriter(f, fieldnames=["source","text"]); w.writeheader()
        for r in all_rows: w.writerow(r)
    print("Saved:", out, "rows:", len(all_rows))

if __name__ == "__main__":
    if MAX_PAGES > 0:
        main()
    else:
        print("MAX_PAGES=0 -> skipping site crawl.")
