import os
from dotenv import load_dotenv
from openai import OpenAI

# load .env from this folder
load_dotenv(dotenv_path=".env")

api_key = os.getenv("OPENAI_API_KEY")
if not api_key:
    raise RuntimeError("OPENAI_API_KEY not found in environment")

client = OpenAI(api_key=api_key)
r = client.responses.create(model="gpt-4.1-mini", input="Say hello in one word.")
print("OK:", r.output_text)
