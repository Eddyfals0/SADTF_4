"""
Persistence utilities for loading and saving configuration and state files.
"""
import json
import os
from pathlib import Path
from typing import Dict, Any, Optional


def get_config_path() -> Path:
    """Get the path to config.json in the project root."""
    return Path(__file__).parent.parent / "config.json"


def get_node_state_path() -> Path:
    """Get the path to node_state.json in the project root."""
    return Path(__file__).parent.parent / "node_state.json"


def get_metadata_path() -> Path:
    """Get the path to metadata.json in the project root."""
    return Path(__file__).parent.parent / "metadata.json"


def load_config() -> Dict[str, Any]:
    """
    Load configuration from config.json.
    Returns default config if file doesn't exist.
    """
    config_path = get_config_path()
    if config_path.exists():
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            pass
    
    # Default configuration
    default_config = {
        "capacity_mb": 50,
        "port": 8888
    }
    save_config(default_config["capacity_mb"], default_config["port"])
    return default_config


def save_config(capacity_mb: int, port: int) -> None:
    """Save configuration to config.json."""
    config_path = get_config_path()
    config = {
        "capacity_mb": capacity_mb,
        "port": port
    }
    with open(config_path, 'w', encoding='utf-8') as f:
        json.dump(config, f, indent=2)


def load_node_state() -> Optional[Dict[str, Any]]:
    """
    Load node state from node_state.json.
    Returns None if file doesn't exist.
    """
    node_state_path = get_node_state_path()
    if node_state_path.exists():
        try:
            with open(node_state_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            pass
    return None


def save_node_state(node_id: int, group_id: str) -> None:
    """Save node state to node_state.json."""
    node_state_path = get_node_state_path()
    state = {
        "node_id": node_id,
        "group_id": group_id
    }
    with open(node_state_path, 'w', encoding='utf-8') as f:
        json.dump(state, f, indent=2)


def load_metadata() -> Dict[str, Any]:
    """
    Load metadata from metadata.json.
    Returns empty dict if file doesn't exist.
    """
    metadata_path = get_metadata_path()
    if metadata_path.exists():
        try:
            with open(metadata_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            pass
    return {"files": {}, "blocks": {}}


def save_metadata(metadata: Dict[str, Any]) -> None:
    """Save metadata to metadata.json."""
    metadata_path = get_metadata_path()
    with open(metadata_path, 'w', encoding='utf-8') as f:
        json.dump(metadata, f, indent=2)

