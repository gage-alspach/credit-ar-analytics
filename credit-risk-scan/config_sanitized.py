"""
Constants + environment config.
"""

import os

ROOT_SCAN_DIR = os.environ.get("CREDIT_RISK_SCAN_ROOT_DIR", r"./data/credit_reviews")
REFERENCE_XLSX_PATH = os.environ.get("CREDIT_RISK_REFERENCE_XLSX_PATH", r"./data/reference/customer_parent_map.xlsx")
ANSONIA_OUTPUT_CSV_PATH = os.environ.get(
    "CREDIT_RISK_AGENCY2_OUTPUT_CSV_PATH", r"./data/output/credit_agency_2_history.csv"
)
DNB_OUTPUT_CSV_PATH = os.environ.get(
    "CREDIT_RISK_AGENCY1_OUTPUT_CSV_PATH", r"./data/output/credit_agency_1_history.csv"
)

ALLOWED_AGENCIES = {"CREDIT_AGENCY_1", "CREDIT_AGENCY_2"}  # Formerly DNB/Ansonia in internal use

DEFAULT_WORKERS = int(os.environ.get("CREDIT_RISK_WORKERS", os.cpu_count() or 4))

LOG_LEVEL = os.environ.get("CREDIT_RISK_LOG_LEVEL", "INFO")
LOG_FORMAT = "%(asctime)s %(levelname)s %(message)s"

# Reference mapping columns (update to match your Excel file)
REFERENCE_SHEET_NAME = os.environ.get(
    "CREDIT_RISK_REFERENCE_SHEET_NAME", "Accounts Master"
)
REFERENCE_KEY_COLUMN = os.environ.get("CREDIT_RISK_REFERENCE_KEY_COLUMN", "Folder Path")
REFERENCE_PARENT_COLUMN = os.environ.get(
    "CREDIT_RISK_REFERENCE_PARENT_COLUMN", "Parent Company"
)
REFERENCE_PARENT_ID_COLUMN = os.environ.get(
    "CREDIT_RISK_REFERENCE_PARENT_ID_COLUMN", "Parent ID"
)

# Stable output schema
ANSONIA_OUTPUT_COLUMNS = [
    "Parent Company",
    "Parent ID",
    "Agency 2 Score",
    "Date of Agency 2 Score",
    "Agency 2 Rating",
    "Filepath",
]

DNB_OUTPUT_COLUMNS = [
    "Parent Company",
    "Parent ID",
    "Agency 1 Score",
    "Date of Agency 1 Score",
    "Max Credit Recommendation (Agency 1)",
    "PAYDEX (Agency 1)",
    "Delinquency Score",
    "Failure Score",
    "Agency 1 Viability Rating",
    "Bankruptcy Found",
    "Agency 1 Rating",
    "Filepath",
]
