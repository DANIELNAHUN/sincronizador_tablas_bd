"""env_helper.py — helper to build SQLAlchemy engines from environment variables."""
import os

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


def get_engine_from_env(var_name: str) -> Engine:
    """Read *var_name* from the environment and return a SQLAlchemy Engine.

    Loads the .env file from the working directory automatically.
    Raises KeyError if the variable is not defined.
    """
    load_dotenv()
    conn_str = os.environ.get(var_name)
    if not conn_str:
        raise KeyError(
            f"Environment variable '{var_name}' is not defined. "
            "Make sure it is set in your .env file or environment."
        )
    return create_engine(conn_str)
