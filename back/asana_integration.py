# asana_integration.py
import os
import time
import re
from typing import Any, Dict, List, Optional
import requests

ASANA_PAT = os.getenv("ASANA_PAT", "").strip()
ASANA_BASE = "https://app.asana.com/api/1.0"
ASANA_CACHE_TTL = int(os.getenv("ASANA_CACHE_TTL", "300"))  # seconds
ASANA_PROJECT_IDS_ENV = os.getenv("ASANA_PROJECT_IDS", "").strip()

# simple in-memory cache: { "projects": {...}, "tasks:<project_gid>": {...} }
_cache: Dict[str, Dict[str, Any]] = {}

def asana_available() -> bool:
    return bool(ASANA_PAT)

def _headers() -> Dict[str, str]:
    if not asana_available():
        raise RuntimeError("ASANA_PAT is not set")
    return {"Authorization": f"Bearer {ASANA_PAT}"}

def _get(path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    url = f"{ASANA_BASE}{path}"
    r = requests.get(url, headers=_headers(), params=params, timeout=60)
    # If you hit 402 here, it would mean a premium-only endpoint.
    r.raise_for_status()
    return r.json()

def list_workspaces() -> List[Dict[str, Any]]:
    data = _get("/workspaces")
    out = []
    for ws in data.get("data", []):
        out.append({"gid": ws.get("gid"), "name": ws.get("name")})
    return out

def list_projects(workspace_gid: str) -> List[Dict[str, Any]]:
    # projects in a workspace
    params = {"workspace": workspace_gid}
    data = _get("/projects", params=params)
    out = []
    # fetch team name if possible (optional; keep simple)
    for p in data.get("data", []):
        out.append({"gid": p.get("gid"), "name": p.get("name")})
    return out

def _list_projects_all_workspaces() -> List[Dict[str, Any]]:
    out = []
    for ws in list_workspaces():
        out.extend(list_projects(ws["gid"]))
    return out

def list_all_projects() -> List[Dict[str, Any]]:
    """
    Returns projects constrained by ASANA_PROJECT_IDS if provided,
    otherwise all projects from all accessible workspaces.
    """
    ids = [s.strip() for s in ASANA_PROJECT_IDS_ENV.split(",") if s.strip()]
    if ids:
        # resolve just those specific projects by gid
        out = []
        for gid in ids:
            try:
                data = _get(f"/projects/{gid}")
                p = data.get("data", {})
                out.append({"gid": p.get("gid"), "name": p.get("name")})
            except Exception:
                # ignore a bad/old gid
                pass
        return out
    # else: list all
    return _list_projects_all_workspaces()

def _cache_get(key: str):
    entry = _cache.get(key)
    if not entry:
        return None
    if (time.time() - entry["ts"]) > entry["ttl"]:
        return None
    return entry["val"]

def _cache_put(key: str, val, ttl: int):
    _cache[key] = {"val": val, "ts": time.time(), "ttl": ttl}

def _list_tasks_for_project(project_gid: str, limit: int = 200) -> List[Dict[str, Any]]:
    """
    Fetch tasks for a project (NOT using premium workspace search).
    We request a reasonable limit (Asana default pagination is 50).
    """
    cache_key = f"tasks:{project_gid}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    # simple single-page fetch (increase if you need pagination)
    params = {
        "limit": min(limit, 200),
        "opt_fields": "name,notes,permalink_url,completed,assignee.name,projects.name"
    }
    data = _get(f"/projects/{project_gid}/tasks", params=params)
    tasks = data.get("data", [])
    _cache_put(cache_key, tasks, ASANA_CACHE_TTL)
    return tasks

def refresh_asana_cache(force: bool = True) -> List[Dict[str, Any]]:
    """
    Preload tasks for configured projects (or all projects if none configured).
    Returns the list of projects it touched.
    """
    projs = list_all_projects()
    if not force:
        return projs
    for p in projs:
        gid = p.get("gid")
        if not gid:
            continue
        try:
            _list_tasks_for_project(gid)
        except Exception:
            # don't fail the whole refresh on one bad project
            pass
    return projs

# ---------------------------
# Q&A helper
# ---------------------------
def _format_projects_bullets(projects: List[Dict[str, Any]]) -> str:
    if not projects:
        return "I couldn’t find any Asana projects."
    lines = ["**Asana projects**", ""]
    for p in projects[:30]:
        lines.append(f"• {p.get('name')} — {p.get('gid')}")
    if len(projects) > 30:
        lines.append("• …")
    return "\n".join(lines)

def _format_tasks_bullets(tasks: List[Dict[str, Any]], header: str = "Asana tasks") -> str:
    if not tasks:
        return "I couldn’t find matching Asana tasks."
    lines = [f"**{header}**", ""]
    for t in tasks[:10]:
        name = (t.get("name") or "").strip()
        url  = (t.get("permalink_url") or "").strip()
        proj = ", ".join([pp.get("name") for pp in (t.get("projects") or []) if pp.get("name")])
        snippet = (t.get("notes") or "").strip()
        snippet = re.sub(r"\s+", " ", snippet)
        if len(snippet) > 140:
            snippet = snippet[:137] + "…"
        line = f"• {name}"
        if proj:
            line += f" — [{proj}]"
        # we don’t print raw URLs (your server will sanitize anyway), so omit url here.
        if snippet:
            line += f"\n  {snippet}"
        lines.append(line)
    if len(tasks) > 10:
        lines.append("• …")
    return "\n".join(lines)

def asana_answer(question: str) -> str:
    """
    Lightweight intent:
      - "list asana projects" / "asana projects": list projects
      - else: keyword search across tasks in configured projects (or all)
    """
    if not asana_available():
        return "Asana is not configured."

    ql = (question or "").lower()

    # list projects intent
    if ("asana" in ql and "project" in ql) or ("list projects" in ql):
        projs = list_all_projects()
        return _format_projects_bullets(projs)

    # keyword task search (non-premium friendly)
    # Extract a simple keyword phrase
    m = re.search(r"(tasks?\s+(about|for|with)\s+)(.+)", ql)
    keyword = (m.group(3).strip() if m else "").strip("'\" ")
    if not keyword:
        # fallback: use whole query as keyword after removing the word 'asana'
        keyword = ql.replace("asana", "").strip()

    # collect tasks from projects, filter locally
    projs = list_all_projects()
    matched: List[Dict[str, Any]] = []
    for p in projs:
        gid = p.get("gid")
        if not gid:
            continue
        try:
            tasks = _list_tasks_for_project(gid)
        except Exception:
            continue
        for t in tasks:
            name = (t.get("name") or "").lower()
            notes = (t.get("notes") or "").lower()
            if keyword and (keyword in name or keyword in notes):
                matched.append(t)

    if not matched:
        # No matches; offer the list of projects as a hint.
        return "I couldn’t find matching Asana tasks.\n\n" + _format_projects_bullets(projs)

    return _format_tasks_bullets(matched, header=f"Asana tasks matching “{keyword}”")
