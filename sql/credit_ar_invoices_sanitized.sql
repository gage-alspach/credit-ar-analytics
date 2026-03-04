-- credit_ar_invoices_sanitized.sql
-- Sanitized example (no real company names, database/catalog names, customer IDs, or proprietary status codes).
-- Target engine: Spark SQL / Databricks SQL (uses functions like MAX_BY, REGEXP_EXTRACT, DATE_SUB).

WITH
/* (0) Optional: hard-coded payment-term overrides as workarounds.
   Ideally this would be a maintained reference table, not literals in SQL. */
terms_overrides AS (
  SELECT * FROM VALUES
    ('CUST_OVERRIDE_A', 45),
    ('CUST_OVERRIDE_B', 60),
    ('CUST_OVERRIDE_C', 120)
  AS t(customer_id, terms_days_override)
),

/* (1) Customer dimension - normalized keys + derived attributes */
dim_customer AS (
  SELECT
    UPPER(TRIM(c.customer_name))                               AS customer_name,
    TRIM(c.customer_id)                                       AS customer_id,
    CAST(c.credit_limit AS DECIMAL(18,2))                     AS credit_limit,

    /* Payment terms:
       - First apply explicit overrides (rare edge cases).
       - Otherwise parse digits from a terms code (e.g., NET30).
       - Default to 30 when missing/unparseable. */
    COALESCE(
      o.terms_days_override,
      TRY_CAST(REGEXP_EXTRACT(UPPER(TRIM(c.payment_terms_code)), '\\d+', 0) AS INT),
      30
    )                                                         AS terms_days,
    CASE
      WHEN c.payment_terms_code IS NULL
        OR TRIM(c.payment_terms_code) = ''
        OR NOT (c.payment_terms_code RLIKE '\\d')
      THEN TRUE ELSE FALSE
    END                                                       AS is_defaulted_terms,

    c.posting_group                                           AS posting_group,
    CAST(c.ar_balance AS DECIMAL(22,2))                       AS ar_balance,

    /* Parent fields are left generic here (in many ERPs parent=customer, or parent comes from a separate mapping) */
    UPPER(TRIM(c.parent_name))                                AS parent_name,
    TRIM(c.parent_id)                                         AS parent_id,

    CASE
      WHEN c.ar_balance <> 0
        OR c.credit_limit <> 0
        OR c.last_modified_at >= DATE_SUB(CURRENT_DATE(), 180)
      THEN TRUE ELSE FALSE
    END                                                       AS is_active_credit,

    CASE
      WHEN c.ar_balance <> 0 THEN 'Balance'
      WHEN c.credit_limit <> 0 THEN 'Credit Limit'
      WHEN c.last_modified_at >= DATE_SUB(CURRENT_DATE(), 180) THEN 'Last Modified'
      ELSE 'Inactive'
    END                                                       AS active_reason,

    UPPER(TRIM(c.company_id))                                 AS company_id,
    CURRENT_DATE()                                            AS as_of_date
  FROM raw.erp_customer c
  LEFT JOIN terms_overrides o
    ON o.customer_id = TRIM(c.customer_id)
  WHERE COALESCE(TRIM(c.customer_name), '') <> ''
    AND NOT (c.customer_name RLIKE '(?i)test' OR c.customer_name RLIKE '(?i)automation')
),

/* (2) Snapshot date = min(requested_as_of, max_posting_date) to prevent “future” as-of dates. */
req AS ( SELECT CURRENT_DATE() AS requested_as_of ),

ledger_detail_dedup AS (
  /* Deduplicate ledger detail snapshots (common in replicated ERP feeds). */
  SELECT *
  FROM (
    SELECT
      l.*,
      ROW_NUMBER() OVER (
        PARTITION BY l.customer_id, l.entry_id
        ORDER BY
          CASE WHEN l.is_processed = TRUE THEN 1 ELSE 0 END DESC,
          COALESCE(l.dwh_modified_at, l.system_modified_at) DESC
      ) AS rn
    FROM raw.erp_customer_ledger_detail l
  ) x
  WHERE x.rn = 1
),

freshness AS (
  SELECT MAX(TO_DATE(posting_date)) AS ledger_max_posting_date
  FROM ledger_detail_dedup
),

dates AS (
  SELECT
    r.requested_as_of,
    f.ledger_max_posting_date,
    CASE
      WHEN r.requested_as_of > f.ledger_max_posting_date THEN f.ledger_max_posting_date
      ELSE r.requested_as_of
    END AS effective_as_of
  FROM req r
  CROSS JOIN freshness f
),

/* (3) Normalize ledger rows to a consistent “document” concept:
   - invoices = positive amounts on invoice-like doc types
   - payments/credits = negative amounts on payment-like doc types
   This is the core step that enables later “open invoice” logic. */
ledger_pt AS (
  SELECT
    UPPER(TRIM(l.company_id))                                  AS company_id,
    TRIM(l.customer_id)                                       AS customer_id,
    l.entry_id                                                AS entry_id,
    l.closed_entry_id                                         AS closed_entry_id,  -- “CLE” concept
    TO_DATE(l.posting_date)                                   AS posting_date,
    TO_DATE(l.due_date)                                       AS due_date,
    UPPER(TRIM(l.document_type))                              AS doc_type_norm,
    TRIM(l.document_number)                                   AS document_number,
    CAST(l.amount AS DECIMAL(18,2))                           AS amount,
    CAST(l.remaining_amount AS DECIMAL(18,2))                 AS remaining_amount,
    CAST(l.original_amount AS DECIMAL(18,2))                  AS original_amount
  FROM ledger_detail_dedup l
  CROSS JOIN dates d
  WHERE TO_DATE(l.posting_date) <= d.effective_as_of
),

/* (4) Collapse many ledger rows into one row per closed_entry_id (“CLE”) to pick canonical doc numbers/dates. */
cle_rollup AS (
  SELECT
    company_id,
    customer_id,
    closed_entry_id,

    /* Canonical invoice doc number/date (debits) */
    MAX_BY(
      CASE
        WHEN doc_type_norm IN ('INVOICE', 'FINANCE CHARGE') AND amount > 0 THEN document_number
        ELSE NULL
      END,
      CASE
        WHEN doc_type_norm IN ('INVOICE', 'FINANCE CHARGE') AND amount > 0 THEN posting_date
        ELSE NULL
      END
    ) AS invoice_document_number,

    MAX(CASE WHEN doc_type_norm IN ('INVOICE','FINANCE CHARGE') THEN posting_date END) AS posting_date,
    MAX(CASE WHEN doc_type_norm IN ('INVOICE','FINANCE CHARGE') THEN due_date END)     AS due_date,

    /* Canonical payment/credit doc number/date (credits) */
    MAX_BY(
      CASE
        WHEN doc_type_norm IN ('PAYMENT', 'CREDIT MEMO', 'REFUND') AND amount < 0 THEN document_number
        ELSE NULL
      END,
      CASE
        WHEN doc_type_norm IN ('PAYMENT', 'CREDIT MEMO', 'REFUND') AND amount < 0 THEN posting_date
        ELSE NULL
      END
    ) AS payment_document_number,

    /* Aggregates across rows */
    SUM(amount)                                              AS cle_net_amount,
    SUM(remaining_amount)                                    AS cle_remaining_amount,
    MAX(posting_date)                                        AS max_any_posting_date,
    MIN(posting_date)                                        AS min_any_posting_date
  FROM ledger_pt
  GROUP BY company_id, customer_id, closed_entry_id
),

/* (5) “Open invoices” - debits with remaining > 0 at the CLE level. */
open_invoices AS (
  SELECT
    p.company_id,
    p.customer_id,
    c.customer_name,
    c.parent_id,
    c.parent_name,
    c.posting_group,
    c.credit_limit,
    c.terms_days,

    p.closed_entry_id,
    p.invoice_document_number                                AS invoice_number,
    p.posting_date                                           AS invoice_posting_date,
    p.due_date                                               AS invoice_due_date,

    p.cle_net_amount                                         AS invoice_amount,
    p.cle_remaining_amount                                   AS remaining_amount,

    /* Days past due as of effective date */
    GREATEST(DATEDIFF((SELECT effective_as_of FROM dates), p.due_date), 0) AS days_past_due
  FROM cle_rollup p
  JOIN dim_customer c
    ON c.company_id = p.company_id AND c.customer_id = p.customer_id
  WHERE p.cle_net_amount > 0
    AND p.cle_remaining_amount > 0
)

SELECT *
FROM open_invoices;
