from pydantic import BaseModel
from langchain_core.prompts import ChatPromptTemplate

from src.models.reflection import ReflectionModel
from src.utils import initialize_llm


llm = initialize_llm()
structured_reflection = llm.with_structured_output(ReflectionModel)

# System prompt specialized for scientific contexts
system = """You are a scientific reviewer evaluating chatbot answers.
Your job is to check whether the answer is factually correct, scientifically valid, and sufficiently precise.
Grade it as 'yes' or 'no', and provide short feedback"""

# Prompt template
reflection_prompt = ChatPromptTemplate.from_messages(
    [
        ("system", system),
        ("human", "Question: {question}\n\nAnswer: {answer}")
    ]
)

# Build chain
reflection_chain = reflection_prompt | structured_reflection