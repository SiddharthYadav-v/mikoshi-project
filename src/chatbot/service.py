from langchain_core.messages import HumanMessage
from .graph import app
from .memory import ChatState


class ChatbotService:
    """
    High-level service for interacting with the chatbot.
    Manages conversation state and provides methods to send/recieve messages.
    """

    def __init__(self):
        # Start with an empty chat state
        self.state: ChatState = {"messages": []}

    def chat(self, user_input: str) -> str:
        """
        Send a user message to the chatbot and return the bot's reply.
        """
        # Add human input
        self.state["messages"].append(HumanMessage(content=user_input))

        # Run through LangGraph app
        self.state = app.invoke(self.state)

        # Get last bot message
        bot_reply = self.state["messages"][-1].content
        return bot_reply
    
    def reset(self):
        """
        Reset the conversation state.
        """
        self.state = {"messages": []}