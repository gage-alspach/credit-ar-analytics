"""
Orchestrates steps 0-8.
"""

import csv
import logging
import os
from datetime import datetime, timezone
from typing import Dict, List

from config import (
    ANSONIA_OUTPUT_COLUMNS,
    ANSONIA_OUTPUT_CSV_PATH,
    DNB_OUTPUT_COLUMNS,
    DNB_OUTPUT_CSV_PATH,
    DEFAULT_WORKERS,
    LOG_FORMAT,
    LOG_LEVEL,
    REFERENCE_KEY_COLUMN,
    REFERENCE_PARENT_COLUMN,
    REFERENCE_PARENT_ID_COLUMN,
    REFERENCE_SHEET_NAME,
    REFERENCE_XLSX_PATH,
    ROOT_SCAN_DIR,
)
from output_index import load_parsed_set
try:
    from parsers.ansonia import parse_ansonia  # type: ignore
    from parsers.dnb import parse_dnb  # type: ignore
except ModuleNotFoundError:
    # Fallback for a flat module layout (parsers/*.py not used)
    from ansonia import parse_ansonia  # type: ignore
    from dnb import parse_dnb  # type: ignore
from reference_map import load_parent_map
from scanner import scan_candidates
from utils_paths import normalize_key

logger = logging.getLogger(__name__)


def _configure_logging() -> None:
    logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)


def _parse_items(items: List[Dict[str, str]]) -> List[Dict[str, str]]:
    results: List[Dict[str, str]] = []
    for item in items:
        pdf_path = item["pdf_path"]
        agency_type = item["agency_type"]
        parse_error = ""
        parse_status = "SUCCESS"
        fields: Dict[str, str] = {}

        try:
            if agency_type == "CREDIT_AGENCY_2":
                fields = parse_ansonia(pdf_path)
            elif agency_type == "CREDIT_AGENCY_1":
                fields = parse_dnb(pdf_path)
        except Exception as exc:  # pylint: disable=broad-except
            parse_status = "FAILED"
            parse_error = str(exc)

        record = {
            **item,
            **fields,
            "parse_status": parse_status,
            "parse_error": parse_error,
            "parsed_at": datetime.now(timezone.utc).isoformat(),
        }
        results.append(record)

    return results


def _enrich_with_parent(
    records: List[Dict[str, str]], parent_map: Dict[str, str]
) -> List[Dict[str, str]]:
    for record in records:
        key = record.get("customer_folder_key", "")
        normalized = normalize_key(key)
        parent_name = parent_map.get(normalized, "")
        record["parent_name"] = parent_name
        record["parent_lookup_status"] = "FOUND" if parent_name else "NOT_FOUND"
    return records


def _write_output(
    records: List[Dict[str, str]], output_path: str, columns: List[str]
) -> None:
    if not records:
        return

    file_exists = os.path.exists(output_path)
    with open(output_path, "a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        if not file_exists:
            writer.writeheader()
        for record in records:
            row = {key: record.get(key, "") for key in columns}
            writer.writerow(row)


def main() -> None:
    _configure_logging()
    logger.info("Starting scan in %s", ROOT_SCAN_DIR)

    parent_company_map = load_parent_map(
        REFERENCE_XLSX_PATH,
        REFERENCE_KEY_COLUMN,
        REFERENCE_PARENT_COLUMN,
        sheet_name=REFERENCE_SHEET_NAME,
    )
    parent_id_map = load_parent_map(
        REFERENCE_XLSX_PATH,
        REFERENCE_KEY_COLUMN,
        REFERENCE_PARENT_ID_COLUMN,
        sheet_name=REFERENCE_SHEET_NAME,
    )
    parsed_set = {
        "CREDIT_AGENCY_2": load_parsed_set(ANSONIA_OUTPUT_CSV_PATH),
        "CREDIT_AGENCY_1": load_parsed_set(DNB_OUTPUT_CSV_PATH),
    }

    scan_stats: Dict[str, int] = {}
    candidates = scan_candidates(ROOT_SCAN_DIR, stats=scan_stats)
    logger.info(
        "Scan stats: files=%s pdfs=%s agency2=%s agency1=%s",
        scan_stats.get("files_scanned"),
        scan_stats.get("pdfs_found"),
        scan_stats.get("agency2_found"),
        scan_stats.get("agency1_found"),
    )

    to_parse = [
        c
        for c in candidates
        if c["pdf_path"] not in parsed_set.get(c["agency_type"], set())
    ]
    logger.info("Candidates=%s, new=%s", len(candidates), len(to_parse))

    if not to_parse:
        logger.info("No new PDFs to parse")
        return

    results = _parse_items(to_parse)
    results = _enrich_with_parent(results, parent_company_map)
    for record in results:
        key = record.get("customer_folder_key", "")
        normalized = normalize_key(key)
        record["parent_id"] = parent_id_map.get(normalized, "")

    ansonia_rows: List[Dict[str, str]] = []
    dnb_rows: List[Dict[str, str]] = []
    for record in results:
        record.setdefault("Parent Company", record.get("parent_name", ""))
        record.setdefault("Parent ID", record.get("parent_id", ""))
        record.setdefault("Filepath", record.get("pdf_path", ""))
        if record.get("agency_type") == "CREDIT_AGENCY_2":
            ansonia_rows.append(record)
        elif record.get("agency_type") == "CREDIT_AGENCY_1":
            dnb_rows.append(record)

    _write_output(ansonia_rows, ANSONIA_OUTPUT_CSV_PATH, ANSONIA_OUTPUT_COLUMNS)
    _write_output(dnb_rows, DNB_OUTPUT_CSV_PATH, DNB_OUTPUT_COLUMNS)

    success_count = sum(1 for r in results if r["parse_status"] == "SUCCESS")
    failure_count = len(results) - success_count
    missing_parent = sum(1 for r in results if r["parent_lookup_status"] == "NOT_FOUND")

    logger.info("Parsed success=%s failure=%s", success_count, failure_count)
    logger.info("Missing parent mappings=%s", missing_parent)
    logger.info("Done")


if __name__ == "__main__":
    main()
