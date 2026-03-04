"""
Output CSV read + parsed_set build.
"""

import csv
import os
from typing import Set


def load_parsed_set(output_csv: str, id_column: str = "Filepath") -> Set[str]:
    if not os.path.exists(output_csv):
        return set()

    with open(output_csv, "r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames or id_column not in reader.fieldnames:
            raise ValueError(f"Missing required column in output CSV: {id_column}")
        return {row[id_column] for row in reader if row.get(id_column)}
