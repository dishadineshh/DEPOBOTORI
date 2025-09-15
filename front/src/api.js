// src/api.js

// Use env var if defined, otherwise fall back to your Render deployment
const API_BASE =
  process.env.REACT_APP_API_BASE || "https://depobotori.onrender.com";

export async function ask(question, opts = {}) {
  const payload = {
    question,
    ...opts, // allows web:true, web_domains:[], etc.
  };

  const res = await fetch(`${API_BASE}/ask`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });

  if (!res.ok) {
    throw new Error(`API ${res.status}: ${await res.text()}`);
  }

  return res.json();
}

export async function status() {
  const res = await fetch(`${API_BASE}/status`);
  if (!res.ok) {
    throw new Error(`API ${res.status}: ${await res.text()}`);
  }
  return res.json();
}
