import logging
import os
import uuid
from contextlib import asynccontextmanager
from typing import List, Literal, Optional

from fastapi import Depends, FastAPI, Header, HTTPException
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from app.config import settings
from app.graph import graph

MAX_MESSAGE_CHARS = 4000
MAX_MESSAGES = 20

logger = logging.getLogger("ecombot")


@asynccontextmanager
async def lifespan(_: FastAPI):
    if not settings.LLM_API_KEY.get_secret_value():
        # Fail fast: without a key every /chat answer would be an error string.
        raise RuntimeError("LLM_API_KEY is not set; refusing to start")
    if not settings.ECOMBOT_API_KEY.get_secret_value():
        logger.warning("ECOMBOT_API_KEY not set — /chat is unauthenticated (demo mode)")
    yield


app = FastAPI(title="Ecommerce Support Agent", version="1.0.0", lifespan=lifespan)


class Message(BaseModel):
    role: Literal["user", "assistant", "system"]
    content: str = Field(..., max_length=MAX_MESSAGE_CHARS)


class ChatRequest(BaseModel):
    # Format 1: eval_service sends {"messages": [...]}
    messages: Optional[List[Message]] = Field(default=None, max_length=MAX_MESSAGES)
    # Format 2: direct call sends {"message": "...", "conversation_history": [...]}
    message: Optional[str] = Field(default=None, max_length=MAX_MESSAGE_CHARS)
    conversation_history: List[Message] = Field(default_factory=list, max_length=MAX_MESSAGES)


class ChatResponse(BaseModel):
    response: str


def require_api_key(
    x_api_key: Optional[str] = Header(default=None),
    authorization: Optional[str] = Header(default=None),
) -> None:
    """Shared-secret check for /chat. Open when ECOMBOT_API_KEY is not configured."""
    expected = settings.ECOMBOT_API_KEY.get_secret_value()
    if not expected:
        return
    presented = x_api_key
    if not presented and authorization and authorization.lower().startswith("bearer "):
        presented = authorization[7:].strip()
    if presented != expected:
        raise HTTPException(status_code=401, detail="Invalid or missing API key")


@app.post("/chat", response_model=ChatResponse, dependencies=[Depends(require_api_key)])
async def chat(request: ChatRequest):
    """Process a chat message through the agent graph."""
    if request.messages:
        messages = [m.model_dump() for m in request.messages]
    elif request.message:
        messages = [m.model_dump() for m in request.conversation_history] + [
            {"role": "user", "content": request.message}
        ]
    else:
        raise HTTPException(status_code=422, detail="No message provided")

    if not any(m["role"] == "user" for m in messages):
        raise HTTPException(status_code=422, detail="Conversation contains no user message")

    initial_state = {"messages": messages, "intent": "", "tool_output": "", "response": ""}

    request_id = uuid.uuid4().hex[:12]
    try:
        # graph.invoke does blocking Snowflake + LLM I/O; keep it off the event loop.
        result = await run_in_threadpool(graph.invoke, initial_state)
        return {"response": result["response"]}
    except Exception:
        logger.exception("Chat processing error (request_id=%s)", request_id)
        return JSONResponse(
            status_code=500,
            content={"response": "Sorry, something went wrong processing your request.", "request_id": request_id},
        )


@app.get("/health")
async def health():
    """Liveness: the process is up."""
    return {"status": "ok"}


@app.get("/ready")
async def ready():
    """Readiness: configuration present and the warehouse answers a trivial query."""
    from app.snowflake_client import ping

    checks = {
        "llm_key_configured": bool(settings.LLM_API_KEY.get_secret_value()),
        "snowflake": await run_in_threadpool(ping),
    }
    status_code = 200 if all(checks.values()) else 503
    return JSONResponse(status_code=status_code, content={"status": "ready" if status_code == 200 else "degraded", "checks": checks})


@app.get("/version")
async def version():
    """Version endpoint returning the git commit SHA."""
    return {"version": os.environ.get("GIT_COMMIT_SHA", "dev")}
