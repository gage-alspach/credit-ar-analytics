## Credit Risk Scan

This folder contains a lightweight Python pipeline that scans external credit-agency reports and converts them into a structured dataset for analytics.

External risk reports are typically delivered as semi-structured documents (PDFs or exports) and vary by provider. This pipeline standardizes those reports into a consistent schema that can be joined with internal AR analytics such as aging, exposure, and payment behavior.

### Workflow

```
Incoming Reports
      │
      ▼
File Scanner
(scanner.py)
      │
      ▼
Agency Detection
(reference_map.py)
      │
      ▼
Agency Parsers
├─ credit_agency1.py
└─ credit_agency2.py
      │
      ▼
Normalized Output Dataset
```

### Key Features

* **Automated report scanning** to detect newly added files
* **Agency-specific parsers** to extract structured signals from different formats
* **Idempotent processing** using an output index to prevent duplicate ingestion
* **Reference mapping** to link agency reports to internal customer identifiers
* **Normalized schema** so multiple credit agencies can be analyzed together

### Example Output Fields

| Column                   | Description                  |
| ------------------------ | ---------------------------- |
| customer_id              | Internal customer identifier |
| parent_id                | Parent company identifier    |
| agency                   | Source credit agency         |
| report_date              | Date of the report           |
| risk_score               | Extracted agency risk score  |
| recommended_credit_limit | Agency credit recommendation |

### Notes

All identifiers, paths, and file names have been generalized.
The repository demonstrates the **architecture and logic of the pipeline**, but contains **no proprietary data or internal system identifiers**.
