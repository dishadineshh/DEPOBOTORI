import React, { useEffect, useMemo, useRef, useState } from "react";
import { ask, status } from "./api";

function Message({ role, text, time, thinking }) {
  const isUser = role === "user";
  const isBot = role === "bot";
  return (
    <div className={`msg ${isUser ? "user" : "bot"}`}>
      <div className="bubble">
        {thinking ? (
          <div className="typing">
            <span className="dot" />
            <span className="dot" />
            <span className="dot" />
          </div>
        ) : (
          <>
            <div className="text">{text}</div>
            {time ? <div className="time">{time}</div> : null}
          </>
        )}
      </div>
    </div>
  );
}

// --- mode keywords ---
const NEWS_KEYWORDS = ["today","latest","this week","breaking","current","news","2025"];
const GA_KEYWORDS   = ["top countries","top pages","busiest","total active users","daily active users","daily users"];

// Suggestions
const seedNewsExamples = [
  "Stock market today",
  "Breaking news on inflation",
  "Latest in AI safety",
  "Nvidia earnings this week",
  "New Android release today",
];

const seedGAExamples = [
  "top pages last 7 days",
  "top countries this week",
  "busiest hour today",
  "daily active users trend",
];

function makeWebSuggestions(q) {
  const base = q ? [q.trim()] : [];
  const news = NEWS_KEYWORDS.map(k => (q ? `${q.trim()} ${k}` : k));
  const curated = seedNewsExamples.filter(s => !q || s.toLowerCase().includes(q.toLowerCase()));
  return [...base, ...news, ...curated]
    .filter(Boolean)
    .filter((s, i, arr) => arr.indexOf(s) === i)
    .slice(0, 10);
}

function makeGASuggestions(q) {
  const base = q ? [q.trim()] : [];
  const ga = GA_KEYWORDS.map(k => (q ? `${k} for ${q.trim()}` : k));
  const curated = seedGAExamples.filter(s => !q || s.toLowerCase().includes(q.toLowerCase()));
  return [...base, ...ga, ...curated]
    .filter(Boolean)
    .filter((s, i, arr) => arr.indexOf(s) === i)
    .slice(0, 10);
}

// --- Typing UX config ---
const MIN_THINK_MS = 600;         // Minimum delay before typing starts (feels intentional)
const MAX_THINK_MS = 1400;        // Maximum initial “thinking” delay
const TYPE_SPEED_CHARS_PER_SEC = 55; // Typing speed
const TYPE_MIN_STEP_MS = 16;         // Min frame (approx 60fps)

export default function App() {
  const [messages, setMessages] = useState([
    {
      role: "system",
      text:
        "Hi! I’m your company’s knowledge companion. Ask me anything about projects, updates, changes, or history — I’ve got it all.",
      time: "",
    },
  ]);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const [healthy, setHealthy] = useState(null);
  const listRef = useRef(null);

  // NEW: UI state for suggestive prompts
  const [mode, setMode] = useState("web"); // "web" | "ga"
  const [openSuggest, setOpenSuggest] = useState(false);
  const [activeIdx, setActiveIdx] = useState(0);

  const suggestions = useMemo(() => {
    return mode === "web" ? makeWebSuggestions(input) : makeGASuggestions(input);
  }, [mode, input]);

  useEffect(() => {
    status()
      .then(() => setHealthy(true))
      .catch(() => setHealthy(false));
  }, []);

  useEffect(() => {
    if (listRef.current) {
      listRef.current.scrollTop = listRef.current.scrollHeight;
    }
  }, [messages, loading]);

  // Utility: push a message
  const pushMessage = (msg) => {
    setMessages((m) => [...m, msg]);
  };

  // Utility: update last message (used for typing animation)
  const updateLastBotMessage = (fn) => {
    setMessages((prev) => {
      const copy = [...prev];
      for (let i = copy.length - 1; i >= 0; i--) {
        if (copy[i].role === "bot") {
          copy[i] = { ...copy[i], ...fn(copy[i]) };
          break;
        }
      }
      return copy;
    });
  };

  // Typewriter reveal
  const typeOut = async (fullText) => {
    // Replace thinking bubble with empty bot message (start typing)
    updateLastBotMessage(() => ({ thinking: false, text: "" }));

    const total = fullText.length;
    if (total === 0) {
      updateLastBotMessage(() => ({ text: fullText, time: nowTime() }));
      return;
    }
    const charsPerMs = TYPE_SPEED_CHARS_PER_SEC / 1000;
    let shown = 0;
    let lastTs = performance.now();

    return new Promise((resolve) => {
      const tick = () => {
        const ts = performance.now();
        const dt = Math.max(TYPE_MIN_STEP_MS, ts - lastTs);
        lastTs = ts;
        const toAdd = Math.max(1, Math.floor(dt * charsPerMs));
        shown = Math.min(total, shown + toAdd);
        const slice = fullText.slice(0, shown);
        updateLastBotMessage(() => ({ text: slice }));
        if (shown < total) {
          requestAnimationFrame(tick);
        } else {
          // stamp time at the end
          updateLastBotMessage(() => ({ time: nowTime() }));
          resolve();
        }
      };
      requestAnimationFrame(tick);
    });
  };

  const nowTime = () =>
    new Date().toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });

  const sendCore = async (q) => {
    // Push user message immediately
    pushMessage({ role: "user", text: q, time: nowTime() });

    // Show a separate thinking bubble WHILE backend works
    setLoading(true);
    pushMessage({ role: "bot", text: "", thinking: true, time: "" });

    try {
      const res = await ask(q);
      const ans = (res?.answer || "No answer.").trim();
      const srcs = (res?.sources || []).filter(Boolean);

      // Apply an intentional “thinking” delay before typing starts
      const thinkDelay =
        MIN_THINK_MS +
        Math.floor(Math.random() * (MAX_THINK_MS - MIN_THINK_MS + 1));
      await new Promise((r) => setTimeout(r, thinkDelay));

      // Typing animation for the main text
      await typeOut(ans);

      // If you want to also “type” the Sources block on the UI side,
      // keep your current “Sources:” UI logic (it appends after answer).
      // Nothing else needed here because your frontend already adds the sources block
      // after receiving the answer string.

      // (No additional message push; we updated the last bot bubble in-place.)
    } catch (e) {
      // Replace thinking with an error bubble
      updateLastBotMessage(() => ({
        thinking: false,
        text: `Error: ${e.message}`,
        time: nowTime(),
      }));
    } finally {
      setLoading(false);
    }
  };

  const onSend = async () => {
    const q = (input || "").trim();
    if (!q || loading) return;

    // Prepend GA tag if in GA mode (optional feature you enabled)
    const payload = mode === "ga" ? `GA: ${q}` : q;

    setOpenSuggest(false);
    setInput("");
    await sendCore(payload);
  };

  const runPrompt = async (text) => {
    if (loading) return;
    if (!text) return;
    const payload = mode === "ga" ? `GA: ${text}` : text;
    setOpenSuggest(false);
    await sendCore(payload);
  };

  const onQuick = (q) => {
    setInput(q);
    setTimeout(() => runPrompt(q), 0);
  };

  const onKeyDown = (e) => {
    if (e.key === "ArrowDown") {
      e.preventDefault();
      setOpenSuggest(true);
      setActiveIdx((i) => Math.min(i + 1, Math.max(0, suggestions.length - 1)));
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      setOpenSuggest(true);
      setActiveIdx((i) => Math.max(i - 1, 0));
    } else if (e.key === "Enter") {
      if (openSuggest && suggestions[activeIdx]) {
        e.preventDefault();
        runPrompt(suggestions[activeIdx]);
      } else {
        onSend();
      }
    } else if (e.key === "Escape") {
      setOpenSuggest(false);
    }
  };

  return (
    <div className="layout">
      <aside className="sidebar">
        <div className="brand">Upload Digital</div>
        <button className="newchat">+ New Chat</button>
        <div className="history-title">Conversation History</div>
      </aside>

      <main className="main">
        <header className="titlebar">
          <div className="title">Data Depository Agent</div>
          <div className={`status ${healthy ? "ok" : healthy === false ? "bad" : ""}`} />
        </header>

        <div className="banner">
          Hi! I’m your company’s knowledge companion. Ask me anything about projects, updates, changes, or history — I’ve got it all.
        </div>

        {/* Mode toggle */}
        <div className="mode-toggle">
          <button
            className={`mode-btn ${mode === "web" ? "active" : ""}`}
            onClick={() => { setMode("web"); setOpenSuggest(false); setActiveIdx(0); }}
          >
            🌐 Web search
          </button>
          <button
            className={`mode-btn ${mode === "ga" ? "active" : ""}`}
            onClick={() => { setMode("ga"); setOpenSuggest(false); setActiveIdx(0); }}
          >
            📈 Google Analytics
          </button>
        </div>

        <section className="chat" ref={listRef}>
          {messages
            .filter((m) => m.role !== "system")
            .map((m, i) => (
              <Message
                key={i}
                role={m.role}
                text={m.text}
                time={m.time}
                thinking={m.thinking}
              />
            ))}
          {/* kept: nothing extra here; thinking bubble is a real message now */}
        </section>

        {/* Composer with suggestive prompts */}
        <div className="composer">
          <div className="input-wrap" onFocus={() => setOpenSuggest(true)}>
            <input
              className="input"
              placeholder={
                mode === "web"
                  ? "Search the web… try: ‘Nvidia earnings this week’"
                  : "Ask GA… try: ‘top pages last 7 days’"
              }
              value={input}
              onChange={(e) => { setInput(e.target.value); setOpenSuggest(true); setActiveIdx(0); }}
              onKeyDown={onKeyDown}
            />
            {openSuggest && suggestions.length > 0 && (
              <ul className="suggest">
                {suggestions.map((s, i) => (
                  <li
                    key={s + i}
                    className={i === activeIdx ? "active" : ""}
                    onMouseEnter={() => setActiveIdx(i)}
                    onMouseDown={(e) => { e.preventDefault(); runPrompt(s); }}
                  >
                    {s}
                  </li>
                ))}
              </ul>
            )}
          </div>
          <button className="send" onClick={onSend} disabled={loading}>
            Send
          </button>
        </div>

        {/* Quick keyword chips per mode */}
        {mode === "web" ? (
          <div className="card">
            <h3>Quick: newsy prompts</h3>
            <p className="muted">Append a recency keyword to focus results.</p>
            <div className="chips">
              {NEWS_KEYWORDS.map(k => (
                <button key={k} className="chip" onClick={() => setInput(inp => (inp ? `${inp} ${k}` : k))}>
                  {k}
                </button>
              ))}
            </div>
            <div className="examples">
              <span className="muted">Examples:</span>
              {seedNewsExamples.slice(0,4).map(ex => (
                <button key={ex} className="example" onClick={() => onQuick(ex)}>{ex}</button>
              ))}
            </div>
          </div>
        ) : (
          <div className="card">
            <h3>Google Analytics prompts</h3>
            <p className="muted">These route to your analytics handler.</p>
            <div className="chips">
              {GA_KEYWORDS.map(k => (
                <button key={k} className="chip" onClick={() => onQuick(k)}>
                  {k}
                </button>
              ))}
            </div>
            <div className="examples">
              <span className="muted">Examples:</span>
              {seedGAExamples.map(ex => (
                <button key={ex} className="example" onClick={() => onQuick(ex)}>{ex}</button>
              ))}
            </div>
          </div>
        )}

        {/* Your original quick buttons */}
        <div className="quick">
          <button onClick={() => onQuick("Show me project updates")}>Show me project updates</button>
          <button onClick={() => onQuick("Who’s working on X?")}>Who’s working on X?</button>
          <button onClick={() => onQuick("Server details")}>Server details</button>
        </div>
      </main>
    </div>
  );
}
