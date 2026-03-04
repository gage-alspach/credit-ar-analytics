"""
Load reference Excel once.
"""

import logging
from typing import Dict, Callable

import pandas as pd

from utils_paths import normalize_key

logger = logging.getLogger(__name__)


def load_parent_map(
    path: str,
    key_column: str,
    parent_column: str,
    sheet_name: str,
    normalize_fn: Callable[[str], str] = normalize_key,
    duplicate_policy: str = "last",
) -> Dict[str, str]:
    df = pd.read_excel(path, sheet_name=sheet_name, dtype=str)
    if key_column not in df.columns or parent_column not in df.columns:
        raise ValueError(f"Missing columns in reference file: {key_column}, {parent_column}")

    parent_map: Dict[str, str] = {}
    duplicates = 0
    total = 0

    for _, row in df.iterrows():
        raw_key = row.get(key_column)
        raw_parent = row.get(parent_column)
        if raw_key is None or raw_parent is None:
            continue
        key = normalize_fn(str(raw_key))
        parent = str(raw_parent).strip()
        if not key:
            continue
        total += 1
        if key in parent_map:
            duplicates += 1
            if duplicate_policy == "first":
                continue
        parent_map[key] = parent

    logger.info("Loaded %s parent mappings", total)
    if duplicates:
        logger.warning("Found %s duplicate keys in reference map", duplicates)

    return parent_map
