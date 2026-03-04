# Architecture

## 1. Goal
Credit AR Analytics provides a reliable, repeatable view of accounts receivable aging, operational exposure, and credit context for customer and parent-level analysis.

This repository documents a reference architecture (system names, file names, and identifiers are generalized).

## 2. High-level data flow
**Source systems**
- ERP System (AR ledger, invoices, payments, credits, credit limits)
- Transportation Management System (shipments and shipment exposure signals)
- Invoice System (not-invoiced amounts, unbilled risk signals)
- Reference files (parent mapping, credit agency history extracts, customer master tracker)

**Processing layer**
- Databricks SQL transformations (raw/bronze -> curated -> gold outputs)

**Semantic layer**
- Power BI dataset (semantic model, measures, relationships)

**Consumption**
- Interactive report (overview and drill-down)
- Paginated reports (detail outputs)

## 3. Core design principles
- **Single source of truth for AR aging**: AR aging is derived from the ERP ledger detail with clear point-in-time logic.
- **Two-anchor modeling strategy**:
  - Customer anchor (CustomerNo) for AR and invoice-level detail
  - Parent anchor (ParentName) for credit agency context and parent rollups
- **Reconciliation-first approach**: each curated output includes validation checks and expected behavior.
- **Operational exposure is directional**: exposure signals are treated as risk indicators (not accounting truth).

## 4. Key outputs (curated datasets)
- Aging (customer-level AR aging + exposure rollup)
- Payment Breakdown (applied payments by month and timeliness bucket)
- Invoices Detail (invoice-level open items and aging buckets)
- Parents (authoritative customer-to-parent mapping)
- Credit Agency History (standardized parent-level history for reporting)
- Credit Master Tracker (bridge between parent naming conventions and entities)

## 5. Assumptions and constraints
- Point-in-time logic uses an effective "as-of" date for consistency.
- Parent naming consistency is critical for agency mapping.
- Some business exceptions (payment term overrides) may exist and should be managed via reference tables over time.

## 6. Security and data handling
This repo contains no proprietary data. Examples, schemas, and naming are generalized and synthetic.
