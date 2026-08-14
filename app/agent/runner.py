from __future__ import annotations

import asyncio
from typing import Optional

from sqlalchemy.orm import Session

from app.agent import config
from app.agent.graph import build_graph
from app.db import get_session

# Compiled once at import: the graph topology is static, so we avoid compiling
# a StateGraph per request (findings F1/E3/C6).
_GRAPH = build_graph()

# Caps concurrent LLM runs so the threadpool / API aren't overwhelmed (REQ-10).
_semaphore = asyncio.Semaphore(config.MAX_CONCURRENCY)


async def run_agent(business_id: int, message: str, history: Optional[list[dict]] = None) -> dict:
    """Run the agent graph on a worker thread with its own DB session.

    A fresh session is opened and closed entirely inside the worker thread:
    - it does not keep the request's session checked out from the pool during
      the slow LLM calls (finding C3: pool exhaustion),
    - it avoids sharing a Session across threads with the request handler.
    """
    async with _semaphore:
        def _run() -> dict:
            db = get_session()
            try:
                state = {
                    "db": db,
                    "business_id": int(business_id),
                    "message": message,
                    "history": history or [],
                }
                return _GRAPH.invoke(state)
            finally:
                db.close()

        result = await asyncio.to_thread(_run)
        return {
            "widgets": result.get("widgets", []),
            "reply": result.get("reply", "No pude responder."),
            "error": result.get("error"),
        }
