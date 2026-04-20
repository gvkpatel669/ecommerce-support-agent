import os
import logging

from fastapi import FastAPI
from pydantic import BaseModel
from typing import Optional

from app.graph import graph

app = FastAPI(title="Ecommerce Support Agent", version="1.0.0")
logger = logging.getLogger("ecombot")


class ChatRequest(BaseModel):
    # Format 1: eval_service sends {"messages": [...]}
    messages: Optional[list] = None
    # Format 2: direct call sends {"message": "...", "conversation_history": [...]}
    message: Optional[str] = None
    conversation_history: list = []


class ChatResponse(BaseModel):
    response: str


@app.on_event("startup")
async def startup():
    from app.config import settings
    if not settings.LLM_API_KEY:
        logger.warning("LLM_API_KEY not set — format_response will fail")


@app.post("/chat")
async def chat(request: ChatRequest):
    """Process a chat message through the agent graph."""
    if request.messages:
        # OpenAI-compatible format from eval_service
        messages = request.messages
    elif request.message:
        messages = request.conversation_history + [{"role": "user", "content": request.message}]
    else:
        return {"response": "No message provided"}

    initial_state = {
        "messages": messages,
        "intent": "",
        "tool_output": "",
        "response": "",
    }

    try:
        result = graph.invoke(initial_state)
        return {"response": result["response"]}
    except Exception as e:
        logger.error(f"Chat processing error: {e}")
        return {"response": f"Error processing request: {str(e)}"}


@app.get("/health")
async def health():
    """Health check endpoint."""
    return {"status": "ok"}


@app.get("/version")
async def version():
    """Version endpoint returning the git commit SHA."""
    return {"version": os.environ.get("GIT_COMMIT_SHA", "dev")}
