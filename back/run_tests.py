# back/run_tests.py
import requests, json, time

API = "http://127.0.0.1:8000/ask"

QUESTIONS = [
    "What services does Upload Digital offer?",
    "Describe Upload Digital in 2–3 sentences.",
    "What makes Upload Digital different from other agencies?",
    "Do you provide email marketing? If yes, what’s your approach?",
    "Do you offer web development? What kind?",
    "List all industries Upload Digital works with.",
    "Summarize the July 10 newsletter about Gen Z attention spans.",
    "What did the newsletter say about vibe marketing?",
    "What guidance was given about meme marketing?",
    "What’s the advice on building a real community vs. just followers?",
    "Which brands were cited (e.g., Duolingo/Nykaa) and why?",
    "Where on the site can I find your service list?",
    "What’s your stance on transparency in pricing/strategy?",
    "Do you have case studies/testimonials? What do they emphasize?",
    "How many LinkedIn rows are recorded in the sheet?",
    "What’s the earliest LinkedIn post date in the sheet?",
    "What’s the latest LinkedIn post date in the sheet?",
    "Give me the titles of the last 3 LinkedIn posts.",
    "Did we post anything in July? Summarize one July post.",
    "List hashtags that appear most frequently (top 3).",
    "How many Instagram entries are there in the sheet?",
    "What’s the latest Instagram post about?",
    "Which posts mention Gen Z?",
    "List any calls-to-action used on Instagram.",
    "Give me 2 post ideas based on what performs well on our Instagram.",
    "Based on newsletters + site, what short-form formats should we prioritize for Gen Z?",
    "Create a one-paragraph pitch for a prospective client referencing our newsletter POV and website services.",
    "From LinkedIn and Instagram sheets, list common themes that align with our service offerings.",
    "If a user asks “What did we launch last July?”, how would you find it? Show sources you’d check first.",
    "What are 3 FAQs a new client might ask us, with answers grounded in our docs?",
    "Do you offer A/B testing?",
    "What’s your pricing?",
    "Who are your exact clients?",
    "Provide 3 quotes from the newsletter about Gen Z (cite the source link).",
    "Provide 3 bullets summarizing our website’s service section (with source link).",
    "Give 2 example captions/topics pulled from LinkedIn sheet rows.",
    "Which week had more social posts: the week of 2024-07-01 or the week after?",
    "Extract any KPIs from the sheets. If none, say so.",
    "Do we have a YouTube channel in the dataset?",
    "What’s the office address?"
]

def ask(q):
    try:
        r = requests.post(API, json={"question": q}, timeout=60)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        return {"error": str(e), "answer": "", "sources": []}

for i, q in enumerate(QUESTIONS, 1):
    print(f"\n[{i}/{len(QUESTIONS)}] Q: {q}")
    res = ask(q)
    if "error" in res and res["error"]:
        print("  ERROR:", res["error"])
        continue
    ans = (res.get("answer") or "").strip()
    sources = res.get("sources", [])
    print("  A:", (ans[:500] + ("..." if len(ans) > 500 else "")))
    print("  Sources:")
    for s in sources:
        print("   -", s)
    time.sleep(0.35)  # small pause to be nice to the API
