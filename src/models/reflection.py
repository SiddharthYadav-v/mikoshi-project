from pydantic import BaseModel, Field
from typing import Literal

class ReflectionModel(BaseModel):
    """Structured output for grading chatbot answers in scientific contexts"""

    score: Literal['yes', 'no'] = Field(description="'yes' or 'no' indicating scientific contexts.")
    explanation: str = Field(description="Brief reasoning for the score")