from langgraph.graph import StateGraph, END
from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage, AIMessage
from .memory import ChatState
from src.utils import initialize_llm

llm = initialize_llm()

def chatbot_node(state: ChatState):
    response = llm.invoke(state["messages"])
    state["messages"].append(response)
    return state


graph = StateGraph(ChatState)
graph.add_node("chatbot", chatbot_node)
graph.set_entry_point("chatbot")
graph.set_finish_point("chatbot")

app = graph.compile()