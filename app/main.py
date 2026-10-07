import hmac
import json
import logging
import os
import uuid
from contextlib import asynccontextmanager
from typing import List, Literal, Optional

from fastapi import Depends, FastAPI, Header, HTTPException
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, field_validator

from app.config import settings
from app.graph import graph
from app.snowflake_client import close as close_snowflake, ping as snowflake_ping

MAX_MESSAGE_CHARS = 4000
MAX_MESSAGES = 20
MAX_BODY_BYTES = 256 * 1024

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(name)s: %(message)s")

logger = logging.getLogger("ecombot")


@asynccontextmanager
async def lifespan(_: FastAPI):
    if not settings.LLM_API_KEY.get_secret_value():
        # Fail fast: without a key every /chat answer would be an error string.
        raise RuntimeError("LLM_API_KEY is not set; refusing to start")
    if not settings.ECOMBOT_API_KEY.get_secret_value():
        logger.warning("ECOMBOT_API_KEY not set — /chat is unauthenticated (demo mode)")
    yield
    await run_in_threadpool(close_snowflake)


app = FastAPI(title="Ecommerce Support Agent", version="1.0.0", lifespan=lifespan)


class BodySizeLimitMiddleware:
    """Pure ASGI middleware: rejects oversized bodies, including chunked ones, before parsing.

    With a Content-Length the check is immediate. Without one (chunked transfer) the body
    is read up to the limit here, rejected with 413 if it exceeds it, and otherwise replayed
    to the application, so memory use is bounded by max_bytes.
    """

    def __init__(self, app, max_bytes: int):
        self.app = app
        self.max_bytes = max_bytes

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        headers = dict(scope.get("headers") or [])
        length = headers.get(b"content-length")
        if length is not None:
            try:
                if int(length) > self.max_bytes:
                    return await self._reject(send, 413, "Request body too large")
            except ValueError:
                return await self._reject(send, 400, "Invalid Content-Length")
            return await self.app(scope, receive, send)

        # No Content-Length: buffer up to the limit, then replay.
        chunks: list = []
        total = 0
        while True:
            message = await receive()
            if message["type"] != "http.request":
                chunks.append(message)
                break
            body = message.get("body", b"")
            total += len(body)
            if total > self.max_bytes:
                return await self._reject(send, 413, "Request body too large")
            chunks.append(message)
            if not message.get("more_body", False):
                break

        async def replay():
            if chunks:
                return chunks.pop(0)
            return await receive()

        return await self.app(scope, replay, send)

    @staticmethod
    async def _reject(send, status: int, detail: str):
        body = json.dumps({"detail": detail}).encode()
        await send({"type": "http.response.start", "status": status,
                    "headers": [(b"content-type", b"application/json"), (b"content-length", str(len(body)).encode())]})
        await send({"type": "http.response.body", "body": body})


app.add_middleware(BodySizeLimitMiddleware, max_bytes=MAX_BODY_BYTES)


class Message(BaseModel):
    role: Literal["user", "assistant", "system"]
    content: str = Field(..., max_length=MAX_MESSAGE_CHARS)

    @field_validator("content", mode="before")
    @classmethod
    def _flatten_multipart(cls, value):
        # OpenAI-style multi-part content: keep the text parts (non-string parts are ignored).
        if isinstance(value, list):
            text = " ".join(str(p.get("text") or "") for p in value if isinstance(p, dict)).strip()
            if not text:
                raise ValueError("content has no text")
            return text
        if not isinstance(value, str):
            raise ValueError("content must be a string")
        return value


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
    if not presented or not hmac.compare_digest(presented.encode("utf-8"), expected.encode("utf-8")):
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
    checks = {
        "llm_key_configured": bool(settings.LLM_API_KEY.get_secret_value()),
        "snowflake": await run_in_threadpool(snowflake_ping),
    }
    status_code = 200 if all(checks.values()) else 503
    return JSONResponse(status_code=status_code, content={"status": "ready" if status_code == 200 else "degraded", "checks": checks})


@app.get("/version")
async def version():
    """Version endpoint returning the git commit SHA."""
    return {"version": os.environ.get("GIT_COMMIT_SHA", "dev")}
