from langchain_core.prompts import ChatPromptTemplate
from src.utils import initialize_llm

llm = initialize_llm()

# System prompt specialized for scientific contexts
system = """You are a helpfule research assistant.
Answer users' questions clearly and concisely, and with scientific accuracy.
If unsure, say you are not sure, instead of guessing."""

generation_prompt = ChatPromptTemplate.from_messages(
    [
        ("system", system),
        ("human", "{question}")
    ]
)

generation_chain = generation_prompt | llm