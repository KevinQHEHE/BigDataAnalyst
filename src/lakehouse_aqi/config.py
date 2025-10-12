"""Configuration loader: YAML file merged with environment variables.

Usage:
    cfg = load_config('configs/app.yaml')
    cfg.get('warehouse_uri')
"""
from typing import Any, Dict
import os
import yaml


def load_yaml(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def flatten_env(prefix: str = "") -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in os.environ.items():
        if prefix and not k.startswith(prefix):
            continue
        out[k] = v
    return out


def merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    """Shallow merge override into base (override wins)."""
    result = dict(base)
    for k, v in override.items():
        result[k] = v
    return result


def load_config(path: str = "configs/config.yaml", env_prefix: str = "") -> Dict[str, Any]:
    """Load configuration from YAML and then overlay environment variables.

    Env vars are read as-is; consumer code should normalize names.
    """
    yaml_cfg = load_yaml(path)
    env_cfg = flatten_env(env_prefix)
    return merge(yaml_cfg, env_cfg)
