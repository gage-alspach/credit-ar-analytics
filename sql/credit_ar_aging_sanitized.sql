-- credit_ar_aging_sanitized.sql
-- Sanitized example (no real company names, database/catalog names, customer IDs, or proprietary status codes).
-- Target engine: Spark SQL / Databricks SQL

WITH
/* (A) CRM-side risk attributes (optional enrichment) */
crm_customer AS (
  SELECT
    COALESCE(a.alt_erp_customer_id, a.erp_customer_id)          AS customer_id,
    CASE
      WHEN a.risk_score = 1 THEN 'LOW'
      WHEN a.risk_score = 2 THEN 'MODERATE'
      WHEN a.risk_score = 3 THEN 'HIGH'
      WHEN a.risk_score = 4 THEN 'HIGH-FLAGGED'
      ELSE NULL
    END                                                        AS internal_risk_bucket,
    a.risk_score                                               AS internal_risk_score
  FROM raw.crm_account a
  WHERE a.account_type = 'CUSTOMER'
    AND a.risk_score IS NOT NULL
),

/* (B) Payment-terms overrides (workaround edge cases), ideally in a reference table. */
terms_overrides AS (
  SELECT * FROM VALUES
    ('CUST_OVERRIDE_A', 45),
    ('CUST_OVERRIDE_B', 60),
    ('CUST_OVERRIDE_C', 120)
  AS t(customer_id, terms_days_override)
),

/* (C) Customer dimension */
dim_customer AS (
  SELECT
    UPPER(TRIM(c.customer_name))                               AS customer_name,
    TRIM(c.customer_id)                                       AS customer_id,
    CAST(c.credit_limit AS DECIMAL(18,2))                     AS credit_limit,
    COALESCE(
      o.terms_days_override,
      TRY_CAST(REGEXP_EXTRACT(UPPER(TRIM(c.payment_terms_code)), '\\d+', 0) AS INT),
      30
    )                                                         AS terms_days,
    c.posting_group                                           AS posting_group,
    CAST(c.ar_balance AS DECIMAL(22,2))                       AS ar_balance,
    UPPER(TRIM(c.parent_name))                                AS parent_name,
    TRIM(c.parent_id)                                         AS parent_id,
    UPPER(TRIM(c.company_id))                                 AS company_id,
    CURRENT_DATE()                                            AS as_of_date,
    crm.internal_risk_score,
    crm.internal_risk_bucket
  FROM raw.erp_customer c
  LEFT JOIN terms_overrides o
    ON o.customer_id = TRIM(c.customer_id)
  LEFT JOIN crm_customer crm
    ON TRIM(crm.customer_id) = TRIM(c.customer_id)
  WHERE COALESCE(TRIM(c.customer_name),'') <> ''
    AND NOT (c.customer_name RLIKE '(?i)test' OR c.customer_name RLIKE '(?i)automation')
    AND c.posting_group IS NOT NULL
),

/* (1) Requested snapshot date */
req AS ( SELECT CURRENT_DATE() AS requested_as_of ),

ledger_detail_dedup AS (
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

/* (2) Ledger, filtered to “as of” date */
ledger_pt AS (
  SELECT
    UPPER(TRIM(l.company_id))                                  AS company_id,
    TRIM(l.customer_id)                                       AS customer_id,
    l.entry_id                                                AS entry_id,
    l.closed_entry_id                                         AS closed_entry_id,
    TO_DATE(l.posting_date)                                   AS posting_date,
    TO_DATE(l.due_date)                                       AS due_date,
    UPPER(TRIM(l.document_type))                              AS doc_type_norm,
    TRIM(l.document_number)                                   AS document_number,
    CAST(l.amount AS DECIMAL(18,2))                           AS amount,
    CAST(l.remaining_amount AS DECIMAL(18,2))                 AS remaining_amount
  FROM ledger_detail_dedup l
  CROSS JOIN dates d
  WHERE TO_DATE(l.posting_date) <= d.effective_as_of
),

/* (3) Roll up by “closed entry” to evaluate open invoices and open credits. */
cle_rollup AS (
  SELECT
    company_id,
    customer_id,
    closed_entry_id,

    /* Debit (invoice-like) aggregates */
    SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END)          AS debit_amount,
    SUM(CASE WHEN amount > 0 THEN remaining_amount ELSE 0 END)AS debit_remaining,

    /* Credit (payment/credit-memo-like) aggregates */
    SUM(CASE WHEN amount < 0 THEN amount ELSE 0 END)          AS credit_amount,
    SUM(CASE WHEN amount < 0 THEN remaining_amount ELSE 0 END)AS credit_remaining,

    MAX(CASE WHEN amount > 0 THEN due_date END)               AS invoice_due_date,
    MAX(CASE WHEN amount > 0 THEN posting_date END)           AS invoice_posting_date
  FROM ledger_pt
  GROUP BY company_id, customer_id, closed_entry_id
),

/* (4) Open invoices (debits with remaining > 0) */
open_invoices AS (
  SELECT
    company_id,
    customer_id,
    closed_entry_id,
    debit_amount                                               AS invoice_amount,
    debit_remaining                                            AS invoice_remaining,
    invoice_posting_date,
    invoice_due_date,
    GREATEST(DATEDIFF((SELECT effective_as_of FROM dates), invoice_due_date), 0) AS days_past_due
  FROM cle_rollup
  WHERE debit_amount > 0
    AND debit_remaining > 0
),

/* (5) Open credits (credits with remaining <> 0). Note remaining is often negative in ERP feeds. */
open_credits AS (
  SELECT
    company_id,
    customer_id,
    closed_entry_id,
    credit_amount                                              AS credit_amount,
    credit_remaining                                           AS credit_remaining
  FROM cle_rollup
  WHERE credit_amount < 0
    AND ABS(credit_remaining) > 0
),

/* (6) Aging buckets */
aging_buckets AS (
  SELECT
    i.company_id,
    i.customer_id,
    SUM(CASE WHEN i.days_past_due = 0 THEN i.invoice_remaining ELSE 0 END)                  AS bucket_current,
    SUM(CASE WHEN i.days_past_due BETWEEN 1 AND 30 THEN i.invoice_remaining ELSE 0 END)     AS bucket_1_30,
    SUM(CASE WHEN i.days_past_due BETWEEN 31 AND 60 THEN i.invoice_remaining ELSE 0 END)    AS bucket_31_60,
    SUM(CASE WHEN i.days_past_due BETWEEN 61 AND 90 THEN i.invoice_remaining ELSE 0 END)    AS bucket_61_90,
    SUM(CASE WHEN i.days_past_due >= 91 THEN i.invoice_remaining ELSE 0 END)                AS bucket_91_plus,
    SUM(i.invoice_remaining)                                                                AS total_open_invoices
  FROM open_invoices i
  GROUP BY i.company_id, i.customer_id
),

credits_by_customer AS (
  SELECT
    company_id,
    customer_id,
    /* Convert credit remaining to a positive “available credits” value for reporting */
    SUM(ABS(credit_remaining)) AS total_open_credits
  FROM open_credits
  GROUP BY company_id, customer_id
)

SELECT
  c.company_id,
  c.customer_id,
  c.customer_name,
  c.parent_id,
  c.parent_name,
  c.posting_group,
  c.credit_limit,
  c.terms_days,
  c.internal_risk_score,
  c.internal_risk_bucket,

  COALESCE(a.bucket_current, 0)            AS aging_current,
  COALESCE(a.bucket_1_30, 0)               AS aging_1_30,
  COALESCE(a.bucket_31_60, 0)              AS aging_31_60,
  COALESCE(a.bucket_61_90, 0)              AS aging_61_90,
  COALESCE(a.bucket_91_plus, 0)            AS aging_91_plus,
  COALESCE(a.total_open_invoices, 0)       AS open_invoices_total,

  COALESCE(cr.total_open_credits, 0)       AS open_credits_total,

  /* Net AR exposure: invoices minus available credits */
  COALESCE(a.total_open_invoices, 0) - COALESCE(cr.total_open_credits, 0) AS net_ar_exposure,

  (SELECT effective_as_of FROM dates)      AS effective_as_of
FROM dim_customer c
LEFT JOIN aging_buckets a
  ON a.company_id = c.company_id AND a.customer_id = c.customer_id
LEFT JOIN credits_by_customer cr
  ON cr.company_id = c.company_id AND cr.customer_id = c.customer_id;
