"""FastAPI surface for the incremental-build adjudicator."""

import os

from fastapi import Body, FastAPI
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import HTMLResponse, JSONResponse

from . import core, db

app = FastAPI(title="增量构建裁决台", docs_url="/api/docs")
app.add_middleware(GZipMiddleware, minimum_size=1024)

STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")


@app.on_event("startup")
def startup():
    db.init_db()


def conn():
    return db.get_conn()


# ---------------------------------------------------------------------------
# pages
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
def index():
    with open(os.path.join(STATIC_DIR, "index.html"), encoding="utf-8") as f:
        return HTMLResponse(f.read())


# ---------------------------------------------------------------------------
# import
# ---------------------------------------------------------------------------

@app.post("/api/import")
def import_graph(payload: dict = Body(...)):
    tasks = payload.get("tasks")
    if not isinstance(tasks, list):
        return JSONResponse({"ok": False, "error": "invalid_payload"}, 400)
    with db.write_lock:
        try:
            result = core.import_graph(conn(), tasks)
        except (ValueError, KeyError, TypeError) as exc:
            return JSONResponse({"ok": False, "error": str(exc)}, 400)
    return {"ok": True, **result}


@app.post("/api/demo")
def demo():
    """Import a generated demo graph so the page has something to show."""
    from .demo import generate
    with db.write_lock:
        result = core.import_graph(conn(), generate())
    return {"ok": True, **result}


# ---------------------------------------------------------------------------
# plan / tasks
# ---------------------------------------------------------------------------

@app.get("/api/plan")
def plan(since: int = 0, limit: int = 0):
    return core.plan_since(conn(), since, limit or None)


@app.get("/api/tasks/{task_id}")
def task(task_id: str):
    detail = core.task_detail(conn(), task_id)
    if detail is None:
        return JSONResponse({"error": "unknown_task"}, 404)
    return detail


@app.get("/api/tasks/{task_id}/chain")
def chain(task_id: str):
    return {"task_id": task_id, "chain": core.reason_chain(conn(), task_id)}


# ---------------------------------------------------------------------------
# claim & report
# ---------------------------------------------------------------------------

@app.post("/api/tasks/{task_id}/claim")
def claim(task_id: str):
    result, err = core.claim(conn(), task_id)
    if err == "unknown_task":
        return JSONResponse({"error": err}, 404)
    if err:
        return JSONResponse({"error": err, **result}, 409)
    return result


@app.post("/api/results")
def report(payload: dict = Body(...)):
    try:
        task_id = payload["task_id"]
        fingerprint = payload["input_fingerprint"]
        reads = payload.get("reads", [])
        artifact = str(payload["artifact_digest"])
    except (KeyError, TypeError):
        return JSONResponse({"status": "error", "error": "invalid_payload"}, 400)
    status, detail = core.report_result(conn(), task_id, fingerprint, reads, artifact)
    code = {"accepted": 200, "stale": 409}.get(status, 400)
    return JSONResponse({"status": status, **detail}, code)
