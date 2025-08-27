from langchain_core.messages import HumanMessage
from typing import Tuple

from src.chatbot.memory import ChatState
from src.chatbot.chains.reflection import reflection_chain

retry_counts = {}

def reflect_node(state: ChatState) -> Tuple[ChatState, dict]:
    """Check if the last AI response is good enough."""
    last_answer = state["messages"][-1].content
    question = next(m.content for m in state["messages"] if isinstance(m, HumanMessage))

    grade = reflection_chain.invoke({"answer": last_answer, "question": question})
    
    reflection_info = {
        "passed": grade.score == "yes",
        "explanation": grade.explanation,
        "should_retry": grade.score != "yes"
    }

    return state, reflection_info