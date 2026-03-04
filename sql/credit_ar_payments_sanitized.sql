-- credit_ar_payments_sanitized.sql
-- Sanitized example (no real company names, database/catalog names, customer IDs).
-- Target engine: Spark SQL / Databricks SQL

WITH
terms_overrides AS (
  SELECT * FROM VALUES
    ('CUST_OVERRIDE_A', 45),
    ('CUST_OVERRIDE_B', 60),
    ('CUST_OVERRIDE_C', 120)
  AS t(customer_id, terms_days_override)
),

dim_customer AS (
  SELECT
    UPPER(TRIM(c.customer_name))                              AS customer_name,
    TRIM(c.customer_id)                                       AS customer_id,
    CAST(c.credit_limit AS DECIMAL(18,2))                     AS credit_limit,
    COALESCE(
      o.terms_days_override,
      TRY_CAST(REGEXP_EXTRACT(UPPER(TRIM(c.payment_terms_code)), '\\d+', 0) AS INT),
      30
    )                                                         AS terms_days,
    c.posting_group                                           AS posting_group,
    UPPER(TRIM(c.company_id))                                 AS company_id
  FROM raw.erp_customer c
  LEFT JOIN terms_overrides o
    ON o.customer_id = TRIM(c.customer_id)
  WHERE COALESCE(TRIM(c.customer_name),'') <> ''
    AND NOT (c.customer_name RLIKE '(?i)test' OR c.customer_name RLIKE '(?i)automation')
    AND c.posting_group IS NOT NULL
),

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

ledger_header_dedup AS (
  /* Some ERPs expose payments in both header + detail tables; this keeps the newest snapshot per entry. */
  SELECT *
  FROM (
    SELECT
      h.*,
      ROW_NUMBER() OVER (
        PARTITION BY h.customer_id, h.entry_id
        ORDER BY COALESCE(h.dwh_modified_at, h.system_modified_at) DESC
      ) AS rn
    FROM raw.erp_customer_ledger h
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

/* Payments = credit-like doc types with negative amounts, open (remaining <> 0) as of effective date. */
open_payments AS (
  SELECT
    UPPER(TRIM(p.company_id))                                  AS company_id,
    TRIM(p.customer_id)                                       AS customer_id,
    UPPER(TRIM(c.customer_name))                              AS customer_name,
    c.posting_group,
    c.terms_days,
    TO_DATE(p.posting_date)                                   AS posting_date,
    UPPER(TRIM(p.document_type))                              AS doc_type_norm,
    TRIM(p.document_number)                                   AS document_number,
    CAST(p.amount AS DECIMAL(18,2))                           AS amount,
    CAST(p.remaining_amount AS DECIMAL(18,2))                 AS remaining_amount
  FROM ledger_header_dedup p
  JOIN dim_customer c
    ON c.company_id = UPPER(TRIM(p.company_id))
   AND c.customer_id = TRIM(p.customer_id)
  CROSS JOIN dates d
  WHERE UPPER(TRIM(p.document_type)) IN ('PAYMENT', 'CREDIT MEMO')
    AND TO_DATE(p.posting_date) <= d.effective_as_of
    AND COALESCE(p.is_open, TRUE) = TRUE
    AND ABS(COALESCE(p.remaining_amount, 0)) > 0
)

SELECT *
FROM open_payments;
