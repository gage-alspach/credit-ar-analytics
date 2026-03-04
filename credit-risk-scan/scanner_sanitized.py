"""
Directory scan + classify agency + candidate build.
"""

import os
import re
from datetime import datetime
from typing import Dict, List, Optional

from config import ALLOWED_AGENCIES
from utils_paths import derive_customer_folder_key

FILENAME_PATTERN = re.compile(
    r"""^\s*
    (?P<name>[A-Z0-9&.,'()\-]+(?:\s+[A-Z0-9&.,'()\-]+)*)   # ALL CAPS name (words)
    \s+
    (?P<m>\d{1,2})\s+(?P<d>\d{1,2})\s+(?P<y>\d{4})         # m d yyyy
    \s*$""",
    re.VERBOSE,
)


def _filename_follows_rule(filename: str) -> bool:
    base, _ext = os.path.splitext(filename)
    base = " ".join(base.split())

    match = FILENAME_PATTERN.match(base)
    if not match:
        return False

    month = int(match.group("m"))
    day = int(match.group("d"))
    year = int(match.group("y"))

    try:
        datetime(year, month, day)
    except ValueError:
        return False

    return True


def _filename_is_dnb(filename: str) -> bool:
    return "-DUNS" in filename.upper()


def classify_agency(filename: str) -> Optional[str]:
    if _filename_follows_rule(filename):
        return "CREDIT_AGENCY_2"
    if _filename_is_dnb(filename):
        return "CREDIT_AGENCY_1"
    return None


def scan_candidates(root_dir: str, stats: Optional[Dict[str, int]] = None) -> List[Dict[str, str]]:
    if stats is None:
        stats = {}
    stats.setdefault("files_scanned", 0)
    stats.setdefault("pdfs_found", 0)
    stats.setdefault("agency2_found", 0)
    stats.setdefault("agency1_found", 0)

    candidates: List[Dict[str, str]] = []
    seen_paths = set()

    def _scan_tree(folder_path: str) -> None:
        with os.scandir(folder_path) as it:
            for entry in it:
                if entry.is_dir(follow_symlinks=False):
                    _scan_tree(entry.path)
                    continue

                if not entry.is_file(follow_symlinks=False):
                    continue

                stats["files_scanned"] += 1
                if not entry.name.lower().endswith(".pdf"):
                    continue

                stats["pdfs_found"] += 1
                agency_type = classify_agency(entry.name)
                if agency_type is None or agency_type not in ALLOWED_AGENCIES:
                    continue

                full_path = entry.path
                if full_path in seen_paths:
                    continue
                seen_paths.add(full_path)

                if agency_type == "CREDIT_AGENCY_2":
                    stats["agency2_found"] += 1
                elif agency_type == "CREDIT_AGENCY_1":
                    stats["agency1_found"] += 1

                candidates.append(
                    {
                        "pdf_path": full_path,
                        "file_name": entry.name,
                        "agency_type": agency_type,
                        "customer_folder_path": folder_path,
                        "customer_folder_key": derive_customer_folder_key(full_path, root_dir),
                    }
                )

                _ = entry.stat().st_mtime

    _scan_tree(root_dir)
    return candidates
