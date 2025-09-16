# asana_integration.py
import os
import time
from typing import Any, Dict, List, Optional
import requests
from dotenv import load_dotenv

load_dotenv()

ASANA_PAT = os.getenv("ASANA_PAT", "").strip()
ASANA_BASE = "https://app.asana.com/api/1.0"

# Optional config
ASANA_PROJECT_IDS = [p.strip() for p in (os.getenv("ASANA_PROJECT_IDS") or "").split(",") if p.strip()]
ASANA_CACHE_TTL = int(os.getenv("ASANA_CACHE_TTL", "300"))  # seconds

_session = requests.Session()
_session.headers.update({"Authorization": f"Bearer {ASANA_PAT}"} if ASANA_PAT else {})

# simple in-memory cache
_cache: Dict[str, Dict[str, Any]] = {}
def _cache_get(key: str):
    item = _cache.get(key)
    if not item: return None
    if time.time() - item["ts"] > ASANA_CACHE_TTL:
        _cache.pop(key, None)
        return None
    return item["value"]

def _cache_set(key: str, value: Any):
    _cache[key] = {"ts": time.time(), "value": value}

def asana_available() -> bool:
    return bool(ASANA_PAT)

def _get(url: str, params: Optional[Dict[str, Any]] = None) -> Any:
    r = _session.get(url, params=params, timeout=60)
    r.raise_for_status()
    return r.json()

def list_workspaces() -> List[Dict[str, Any]]:
    key = "workspaces"
    cached = _cache_get(key)
    if cached is not None:
        return cached
    data = _get(f"{ASANA_BASE}/workspaces")
    workspaces = data.get("data", [])
    _cache_set(key, workspaces)
    return workspaces

def list_projects(workspace_gid: str) -> List[Dict[str, Any]]:
    key = f"projects:{workspace_gid}"
    cached = _cache_get(key)
    if cached is not None:
        return cached
    # include team.name for clarity if available
    data = _get(f"{ASANA_BASE}/projects", params={"workspace": workspace_gid, "archived": False})
    projects = data.get("data", [])
    _cache_set(key, projects)
    return projects

def list_project_tasks(project_gid: str, limit: int = 30) -> List[Dict[str, Any]]:
    """
    Uses the standard (free) endpoint to list tasks in a project.
    We then fetch minimal fields for each task (name/notes/permalink_url).
    """
    tasks: List[Dict[str, Any]] = []

    # 1) list task GIDs in the project
    url = f"{ASANA_BASE}/projects/{project_gid}/tasks"
    params = {"limit": min(limit, 50)}  # Asana paginates at 50 max
    while True and len(tasks) < limit:
        resp = _get(url, params=params)
        data = resp.get("data", [])
        for item in data:
            tasks.append(item)
            if len(tasks) >= limit:
                break
        # pagination
        next_page = (resp.get("next_page") or {}).get("uri")
        if not next_page or len(tasks) >= limit:
            break
        url = next_page
        params = None  # next_page already has query

    # 2) hydrate each task for details (free endpoint)
    detailed: List[Dict[str, Any]] = []
    for t in tasks:
        gid = t.get("gid")
        if not gid:
            continue
        detail = _get(f"{ASANA_BASE}/tasks/{gid}", params={
            "opt_fields": "name,notes,permalink_url,projects.name,completed,due_on,assignee.name"
        }).get("data", {})
        detailed.append(detail)

    return detailed

def _keyword_match(task: Dict[str, Any], q: str) -> bool:
    q = q.lower()
    name = (task.get("name") or "").lower()
    notes = (task.get("notes") or "").lower()
    if q in name or q in notes:
        return True
    # also match project names
    for p in (task.get("projects") or []):
        if q in ((p.get("name") or "").lower()):
            return True
    return False

def search_tasks_keyword(keyword: str, project_ids: Optional[List[str]] = None, per_project: int = 20, max_total: int = 40) -> List[Dict[str, Any]]:
    """
    Free-plan friendly search:
      - list tasks per project
      - client-side filter by keyword (name/notes/project)
    """
    projects = project_ids or ASANA_PROJECT_IDS
    if not projects:
        # Last resort: scan all projects in first workspace
        wss = list_workspaces()
        if not wss:
            return []
        projects_all = list_projects(wss[0]["gid"])
        projects = [p["gid"] for p in projects_all]

    out: List[Dict[str, Any]] = []
    for pid in projects:
        try:
            tasks = list_project_tasks(pid, limit=per_project)
            for t in tasks:
                if _keyword_match(t, keyword):
                    out.append(t)
                    if len(out) >= max_total:
                        return out
        except requests.HTTPError:
            # Skip projects we can't access
            continue
    return out

def projects_summary(project_ids: Optional[List[str]] = None) -> str:
    pids = project_ids or ASANA_PROJECT_IDS
    if not pids:
        wss = list_workspaces()
        if not wss:
            return "I couldn’t find any Asana workspaces."
        projects = list_projects(wss[0]["gid"])
        if not projects:
            return "I couldn’t find any Asana projects."
        lines = ["**Asana projects (sample)**"]
        for p in projects[:25]:
            lines.append(f"• {p.get('name')} (gid: {p.get('gid')})")
        return "\n".join(lines)

    lines = ["**Configured Asana projects**"]
    for gid in pids:
        lines.append(f"• {gid}")
    return "\n".join(lines)

def format_tasks(tasks: List[Dict[str, Any]]) -> str:
    if not tasks:
        return "No matching tasks found."
    lines = ["**Asana tasks (matching)**"]
    for t in tasks[:20]:
        name = t.get("name") or "(no title)"
        url = t.get("permalink_url") or ""
        proj = ", ".join([p.get("name") for p in (t.get("projects") or []) if p.get("name")]) or "(no project)"
        due = t.get("due_on") or ""
        comp = "✅" if t.get("completed") else "⬜"
        lines.append(f"• {comp} {name} — {proj}" + (f" — due {due}" if due else "") + (f"\n   {url}" if url else ""))
    return "\n".join(lines)

def asana_answer(question: str) -> Optional[str]:
    """
    Very simple router: if the question mentions 'asana' or a configured project,
    return projects list or keyword search results.
    """
    ql = (question or "").lower()
    if "asana" not in ql:
        # also trigger if any known project name keyword is in the query
        pass

    # common intents
    if "list projects" in ql or "projects list" in ql:
        return projects_summary()

    # keyword-style: "asana tasks about X", "tasks about XO Curls", etc.
    for hint in ["tasks about", "tasks on", "tasks for", "search tasks", "find tasks"]:
        if hint in ql:
            kw = question.lower().split(hint, 1)[1].strip(" :\"'").strip()
            if kw:
                tasks = search_tasks_keyword(kw)
                return format_tasks(tasks)

    # fallback: if user says "asana" but no hint, show projects
    if "asana" in ql:
        return projects_summary()

    return None

def refresh_asana_cache():
    _cache.clear()
