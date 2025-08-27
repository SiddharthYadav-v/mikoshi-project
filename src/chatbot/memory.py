from typing import TypedDict, List, Dict
from langchain_core.messages import BaseMessage


class ChatState(TypedDict):
    messages: List[BaseMessage]  # List of LangChain messages (HumanMessage, AIMessage, etc.)