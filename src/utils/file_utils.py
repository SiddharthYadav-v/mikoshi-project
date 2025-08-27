import os


def is_s3_path(path: str) -> bool:
    """Check if path is an S3 path."""
    return path.startswith("s3://")


def validate_config_file(config_path: str | None) -> None:
    """
    Validate configuration file exists if provided.

    Args:
        config_path: Path to configuration file

    Raises:
        FileNotFoundError: If config file doesn't exist
    """
    if config_path and not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
