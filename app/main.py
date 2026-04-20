import os

from fastapi import FastAPI
from pydantic import BaseModel

from app.graph import graph

app = FastAPI(title="Ecommerce Support Agent", version="1.0.0")


class ChatRequest(BaseModel):
    message: str
    conversation_history: list = []


class ChatResponse(BaseModel):
    response: str


@app.post("/chat", response_model=ChatResponse)
async def chat(request: ChatRequest):
    """Process a chat message through the agent graph."""
    messages = request.conversation_history + [{"role": "user", "content": request.message}]

    initial_state = {
        "messages": messages,
        "intent": "",
        "tool_output": "",
        "response": "",
    }

    result = graph.invoke(initial_state)
    return ChatResponse(response=result["response"])


@app.get("/health")
async def health():
    """Health check endpoint."""
    return {"status": "ok"}


@app.get("/version")
async def version():
    """Version endpoint returning the git commit SHA."""
    return {"version": os.environ.get("GIT_COMMIT_SHA", "dev")}
