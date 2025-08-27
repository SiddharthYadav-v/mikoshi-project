import yaml
import os
from pathlib import Path
from langchain_openai import ChatOpenAI
from langchain_aws import ChatBedrockConverse
from langchain_google_genai import ChatGoogleGenerativeAI

CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "config.yaml"


def load_config():
    """
    Load configuration from config.yaml as dict.
    """
    with open(CONFIG_PATH, "r") as f:
        return yaml.safe_load(f)


def initialize_llm():
    """
    Initialize an LLM client based on config.yaml.
    Supports: OPENAI, BEDROCK, GEMINI, SLIP.
    """
    config = load_config()
    client_cfg = config["llm_config"]["client"]
    client_name = client_cfg["name"].upper()

    if client_name == "OPENAI":
        model_cfg = client_cfg["openai"]
        return ChatOpenAI(
            model=model_cfg["model_name"],
            temperature=model_cfg["model_params"].get("temperature", 0),
            max_retries=model_cfg["model_params"].get("max_retries", 3),
            max_tokens=model_cfg["model_params"].get("max_output_tokens", 2048),  # type: ignore
            top_p=model_cfg["model_params"].get("top_p", 1.0),
        )
    
    elif client_name == "BEDROCK":
        model_cfg = client_cfg["bedrock"]
        return ChatBedrockConverse(
            model=model_cfg["model_name"],
            temperature=model_cfg["model_params"].get("temperature", 0),
            max_tokens=model_cfg["model_params"].get("max_gen_len", 2048),
        )

    elif client_name == "GEMINI":
        model_cfg = client_cfg["gemini"]
        return ChatGoogleGenerativeAI(
            model=model_cfg["model_name"],
            temperature=model_cfg["model_params"].get("temperature", 0),
            max_output_tokens=model_cfg["model_params"].get("max_output_tokens", 2048),
        )
        
    elif client_name == "SLIP":
        raise NotImplementedError("SLIP client not implemented yet.")
    
    else:
        raise ValueError(f"Unsupported LLM client: {client_name}")