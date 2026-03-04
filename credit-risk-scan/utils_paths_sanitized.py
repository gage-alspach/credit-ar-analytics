"""
Path normalization + key derivation.
"""

import os


def normalize_key(value: str) -> str:
    if value is None:
        return ""
    text = value.strip().replace("/", "\\")
    while "\\\\" in text:
        text = text.replace("\\\\", "\\")
    return text.upper()


def derive_customer_folder_key(pdf_path: str, root_dir: str) -> str:
    parent_dir = os.path.dirname(pdf_path)
    rel_parent = os.path.relpath(parent_dir, root_dir)
    if rel_parent in (".", ""):
        return normalize_key(root_dir)

    first_segment = rel_parent.split(os.sep, 1)[0]
    full_key_path = os.path.join(root_dir, first_segment)
    return normalize_key(full_key_path)
