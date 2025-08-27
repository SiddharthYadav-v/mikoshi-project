from langchain_core.messages import AIMessage

from src.chatbot.memory import ChatState
from src.chatbot.chains.generation import generation_chain

def generate_node(state: ChatState):
    """Generate an AI response from the last human message"""
    question = state["messages"][-1].content
    response = generation_chain.invoke({"question": question})

    response_content = response.content if hasattr(response, 'content') else str(response)
    state["messages"].append(AIMessage(content=response_content))
    
    return state