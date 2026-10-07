import logging
import re
from functools import lru_cache
from typing import TypedDict

from langchain_openai import ChatOpenAI
from langgraph.graph import END, StateGraph

from app.config import settings
from app.prompts import SYSTEM_PROMPT
from app.routing import classify_intent
from app.tools.calculate_profit import calculate_profit
from app.tools.lookup_customer import lookup_customer
from app.tools.query_inventory import query_inventory
from app.tools.query_sales import query_sales

logger = logging.getLogger("ecombot.graph")

# Keep the warehouse context the LLM sees bounded so a huge tool output cannot
# blow the prompt budget.
MAX_TOOL_OUTPUT_CHARS = 6000
MAX_HISTORY_MESSAGES = 10

# Reasoning models (e.g. MiniMax) may prefix answers with <think>…</think>; never expose it.
_THINK_BLOCK = re.compile(r"<think>.*?(?:</think>\s*|\Z)", re.DOTALL | re.IGNORECASE)


def strip_reasoning(text: str) -> str:
    return _THINK_BLOCK.sub("", text or "").strip()


class AgentState(TypedDict):
    messages: list  # conversation history as [{"role": ..., "content": ...}]
    intent: str  # classified intent
    tool_output: str  # raw tool output
    response: str  # final formatted response


TOOL_MAP = {
    "sales": query_sales,
    "inventory": query_inventory,
    "profit": calculate_profit,
    "customer": lookup_customer,
}


def _message_content(msg) -> str:
    if isinstance(msg, dict):
        content = msg.get("content", "")
    else:
        content = getattr(msg, "content", msg)
    if isinstance(content, list):  # OpenAI multi-part content
        content = " ".join(
            str(part.get("text") or "") for part in content if isinstance(part, dict)
        )
    return content if isinstance(content, str) else str(content or "")


def _last_user_index(messages: list) -> int:
    for i in range(len(messages or []) - 1, -1, -1):
        msg = messages[i]
        role = msg.get("role") if isinstance(msg, dict) else getattr(msg, "role", None)
        if role == "user":
            return i
    return len(messages) - 1 if messages else -1


def last_user_message(messages: list) -> str:
    """Return the content of the most recent user turn (falls back to the last message)."""
    idx = _last_user_index(messages)
    return _message_content(messages[idx]) if idx >= 0 else ""


@lru_cache(maxsize=1)
def get_llm() -> ChatOpenAI:
    """Build the chat client once; bounded timeout, one retry, capped output."""
    return ChatOpenAI(
        model=settings.LLM_MODEL,
        api_key=settings.LLM_API_KEY.get_secret_value() or None,
        base_url=settings.LLM_BASE_URL or None,
        temperature=0.3,
        timeout=settings.LLM_TIMEOUT_SECONDS,
        max_retries=1,
        max_tokens=settings.LLM_MAX_TOKENS,
    )


def route_node(state: AgentState) -> AgentState:
    """Classify the user's intent based on the latest user message."""
    intent = classify_intent(last_user_message(state["messages"]))
    return {**state, "intent": intent}


def execute_tool_node(state: AgentState) -> AgentState:
    """Execute the appropriate tool based on classified intent."""
    intent = state["intent"]
    question = last_user_message(state["messages"])

    if intent == "general":
        return {**state, "tool_output": "No specific data tool matched. Answering from general knowledge."}

    tool = TOOL_MAP.get(intent)
    if tool is None:
        return {**state, "tool_output": "No tool available for this intent."}

    try:
        result = tool.invoke(question)
        return {**state, "tool_output": str(result)[:MAX_TOOL_OUTPUT_CHARS]}
    except Exception:
        # Log the real cause server-side; never feed raw exception text to the LLM or caller.
        logger.exception("Tool %s failed", intent)
        return {**state, "tool_output": "Data is temporarily unavailable for this question."}


def format_response_node(state: AgentState) -> AgentState:
    """Use LLM to format tool output into a natural language response."""
    question = last_user_message(state["messages"])
    tool_output = state["tool_output"]

    # Bounded prior turns so follow-ups like "and last week?" keep their context.
    msgs = state["messages"] or []
    idx = _last_user_index(msgs)
    history = [
        {"role": m.get("role", "user"), "content": _message_content(m)}
        for m in msgs[max(0, idx - MAX_HISTORY_MESSAGES):idx]
        if isinstance(m, dict) and m.get("role") in ("user", "assistant")
    ] if idx > 0 else []

    prompt_messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        *history,
        {"role": "user", "content": f"User question: {question}\n\nData from warehouse:\n{tool_output}\n\nPlease provide a helpful response based on this data."},
    ]

    try:
        response = get_llm().invoke(prompt_messages)
        content = response.content if isinstance(response.content, str) else str(response.content)
        answer = strip_reasoning(content)
        if not answer:
            # Reasoning cut off by max_tokens with no visible answer: say so instead of returning "".
            logger.warning("LLM returned no answer text after stripping reasoning")
            return {**state, "response": "I could not generate a response right now. Please try again."}
        return {**state, "response": answer}
    except Exception:
        logger.exception("LLM formatting failed")
        return {**state, "response": "I could not generate a response right now. Please try again."}


def build_graph():
    workflow = StateGraph(AgentState)
    workflow.add_node("route", route_node)
    workflow.add_node("execute_tool", execute_tool_node)
    workflow.add_node("format_response", format_response_node)
    workflow.set_entry_point("route")
    workflow.add_edge("route", "execute_tool")
    workflow.add_edge("execute_tool", "format_response")
    workflow.add_edge("format_response", END)
    return workflow.compile()


graph = build_graph()
