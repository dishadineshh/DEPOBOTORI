# asana_integration.py
import os
import time
from typing import Any, Dict, List, Optional
import requests

ASANA_PAT = os.getenv("ASANA_PAT", "").strip()
ASANA_BASE = "https://app.asana.com/api/1.0"
ASANA_PROJECT_IDS = [s.strip() for s in (os.getenv("ASANA_PROJECT_IDS", "") or "").split(",") if s.strip()]
ASANA_CACHE_TTL = int(os.getenv("ASANA_CACHE_TTL", "300") or "300")  # seconds

_session = requests.Session()
_session.headers.update({
    "Authorization": f"Bearer {ASANA_PAT}" if ASANA_PAT else "",
    "Content-Type": "application/json",
})

_cache: Dict[str, Any] = {
    "ts": 0.0,
    "workspaces": None,   # list
    "projects_by_ws": {}, # {workspace_gid: [projects]}
}

def asana_available() -> bool:
    return bool(ASANA_PAT)

def _now() -> float:
    return time.time()

def _expired(ts: float) -> bool:
    return (_now() - ts) > ASANA_CACHE_TTL

def _get(url: str, params: Optional[Dict[str, Any]] = None, timeout: int = 30) -> Dict[str, Any]:
    if not asana_available():
        raise RuntimeError("ASANA_PAT is not set")
    r = _session.get(url, params=params or {}, timeout=timeout)
    if r.status_code == 401:
        raise RuntimeError("Asana auth failed (401). Check ASANA_PAT.")
    r.raise_for_status()
    return r.json()

# ---------------------------
# Workspaces & Projects
# ---------------------------
def list_workspaces(force: bool = False) -> List[Dict[str, Any]]:
    """[{ gid, name }]"""
    if not asana_available():
        return []
    if (not force) and _cache["workspaces"] and not _expired(_cache["ts"]):
        return _cache["workspaces"]

    data = _get(f"{ASANA_BASE}/workspaces")
    wss = data.get("data", []) or []
    _cache["workspaces"] = [{"gid": w["gid"], "name": w["name"]} for w in wss]
    _cache["ts"] = _now()
    return _cache["workspaces"]

def list_projects(workspace_gid: str, force: bool = False) -> List[Dict[str, Any]]:
    """Projects in a workspace: [{ gid, name, archived, team? }]"""
    if not asana_available():
        return []
    by_ws = _cache["projects_by_ws"].get(workspace_gid)
    if (not force) and by_ws and not _expired(_cache["ts"]):
        return by_ws

    # Newer Asana endpoint: /workspaces/{gid}/projects
    params = {"archived": "false", "limit": 100, "opt_fields": "name,archived,team.name"}
    data = _get(f"{ASANA_BASE}/workspaces/{workspace_gid}/projects", params=params)
    projs = data.get("data", []) or []
    norm = []
    for p in projs:
        norm.append({
            "gid": p.get("gid"),
            "name": p.get("name"),
            "archived": bool(p.get("archived", False)),
            "team": (p.get("team") or {}).get("name"),
        })
    _cache["projects_by_ws"][workspace_gid] = norm
    _cache["ts"] = _now()
    return norm

def list_all_projects(force: bool = False) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for w in list_workspaces(force=force):
        out.extend(list_projects(w["gid"], force=force))
    # If ASANA_PROJECT_IDS is set, filter to those
    if ASANA_PROJECT_IDS:
        out = [p for p in out if (p.get("gid") or "") in ASANA_PROJECT_IDS]
    return out

def refresh_asana_cache(force: bool = True):
    _cache["ts"] = 0.0
    _cache["workspaces"] = None
    _cache["projects_by_ws"] = {}
    return list_all_projects(force=True)

# ---------------------------
# Tasks search
# ---------------------------
def _fetch_task_detail(task_gid: str) -> Dict[str, Any]:
    params = {
        "opt_fields": ",".join([
            "name","notes","permalink_url","completed","due_on",
            "assignee.name","projects.name","created_at","modified_at"
        ])
    }
    data = _get(f"{ASANA_BASE}/tasks/{task_gid}", params=params)
    return data.get("data", {}) or {}

def search_tasks_text(query: str, workspace_gid: str, project_gids: Optional[List[str]] = None, limit: int = 20) -> List[Dict[str, Any]]:
    """
    Text search within a workspace.
    Uses /workspaces/{gid}/tasks/search with 'text=' and optional 'projects.any=' filters.
    """
    params: Dict[str, Any] = {
        "text": query,
        "limit": min(limit, 100),
        "opt_fields": "name,notes,permalink_url,projects.name"
    }
    # If user limited which projects matter
    if project_gids:
        # Asana supports projects.any[]= for multiple
        for i, gid in enumerate(project_gids):
            params[f"projects.any[{i}]"] = gid

    data = _get(f"{ASANA_BASE}/workspaces/{workspace_gid}/tasks/search", params=params)
    tasks = data.get("data", []) or []
    # Return minimal info (no extra per-task GET for speed)
    out = []
    for t in tasks:
        out.append({
            "gid": t.get("gid"),
            "name": t.get("name"),
            "permalink_url": t.get("permalink_url"),
            "projects": [p.get("name") for p in (t.get("projects") or []) if p],
            "snippet": (t.get("notes") or "").splitlines()[0][:180] if t.get("notes") else "",
        })
    return out

def asana_answer(query: str) -> str:
    """
    Very simple routing:
    - If user asks for 'projects', list projects.
    - Else: do a text search across each workspace (respect ASANA_PROJECT_IDS if set).
    """
    if not asana_available():
        return "Asana integration is not configured. Please set ASANA_PAT."

    ql = (query or "").lower()

    # List projects quickly
    if "project" in ql or "projects" in ql or "list projects" in ql:
        projs = list_all_projects()
        if not projs:
            return "I couldn’t find any Asana projects."
        lines = ["**Asana projects**"]
        for p in projs[:50]:
            team = f" — {p['team']}" if p.get("team") else ""
            lines.append(f"• {p['name']}{team} (gid={p['gid']})")
        if len(projs) > 50:
            lines.append("• …")
        return "\n".join(lines)

    # Otherwise: text search (tasks) per workspace
    workspaces = list_workspaces()
    if not workspaces:
        return "I couldn’t access any Asana workspaces."

    all_hits: List[Dict[str, Any]] = []
    for w in workspaces:
        hits = search_tasks_text(
            query=query,
            workspace_gid=w["gid"],
            project_gids=(ASANA_PROJECT_IDS if ASANA_PROJECT_IDS else None),
            limit=10,
        )
        all_hits.extend([{"ws": w["name"], **h} for h in hits])

    if not all_hits:
        return "No matching Asana tasks found."

    lines = ["**Matching Asana tasks**"]
    for h in all_hits[:20]:
        proj_str = f" — {', '.join(h['projects'])}" if h.get("projects") else ""
        lines.append(f"• {h['name']}{proj_str}")
        if h.get("snippet"):
            lines.append(f"  · {h['snippet']}")
    if len(all_hits) > 20:
        lines.append("• …")
    return "\n".join(lines)
