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


class AgentState(TypedDict):
    messages: list  # conversation history
    intent: str  # classified intent
    tool_output: str  # raw tool output
    response: str  # final formatted response


TOOL_MAP = {
    "sales": query_sales,
    "inventory": query_inventory,
    "profit": calculate_profit,
    "customer": lookup_customer,
}


def route_node(state: AgentState) -> AgentState:
    """Classify the user's intent based on the latest message."""
    messages = state["messages"]
    last_message = messages[-1] if messages else ""
    if isinstance(last_message, dict):
        last_message = last_message.get("content", "")
    intent = classify_intent(last_message)
    return {**state, "intent": intent}


def execute_tool_node(state: AgentState) -> AgentState:
    """Execute the appropriate tool based on classified intent."""
    intent = state["intent"]
    messages = state["messages"]
    last_message = messages[-1] if messages else ""
    if isinstance(last_message, dict):
        last_message = last_message.get("content", "")

    if intent == "general":
        return {**state, "tool_output": "No specific data tool matched. Answering from general knowledge."}

    tool = TOOL_MAP.get(intent)
    if tool is None:
        return {**state, "tool_output": "No tool available for this intent."}

    try:
        result = tool.invoke(last_message)
        return {**state, "tool_output": str(result)}
    except Exception as e:
        return {**state, "tool_output": f"Error querying data: {str(e)}"}


def format_response_node(state: AgentState) -> AgentState:
    """Use LLM to format tool output into a natural language response."""
    messages = state["messages"]
    last_message = messages[-1] if messages else ""
    if isinstance(last_message, dict):
        last_message = last_message.get("content", "")

    tool_output = state["tool_output"]

    llm = ChatOpenAI(
        model=settings.LLM_MODEL,
        api_key=settings.LLM_API_KEY,
        temperature=0.3,
    )

    prompt_messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": f"User question: {last_message}\n\nData from warehouse:\n{tool_output}\n\nPlease provide a helpful response based on this data."},
    ]

    try:
        response = llm.invoke(prompt_messages)
        return {**state, "response": response.content}
    except Exception as e:
        return {**state, "response": f"I encountered an error generating a response: {str(e)}"}


# Build the graph
workflow = StateGraph(AgentState)

workflow.add_node("route", route_node)
workflow.add_node("execute_tool", execute_tool_node)
workflow.add_node("format_response", format_response_node)

workflow.set_entry_point("route")
workflow.add_edge("route", "execute_tool")
workflow.add_edge("execute_tool", "format_response")
workflow.add_edge("format_response", END)

graph = workflow.compile()
