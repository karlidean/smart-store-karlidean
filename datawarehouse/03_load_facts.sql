PRAGMA foreign_keys = ON;

-- Keep a rejects table for troubleshooting
CREATE TABLE IF NOT EXISTS fact_sales_rejects AS
SELECT
  CAST(NULL AS INTEGER) AS transaction_id,
  CAST(NULL AS INTEGER) AS date_key,
  CAST(NULL AS INTEGER) AS customer_key,
  CAST(NULL AS INTEGER) AS product_key,
  CAST(NULL AS INTEGER) AS store_key,
  CAST(NULL AS INTEGER) AS campaign_key,
  CAST(NULL AS REAL)    AS sale_amount,
  CAST(NULL AS REAL)    AS percent_discount,
  CAST(NULL AS REAL)    AS sale_final,
  CAST(NULL AS INTEGER) AS paid_with_points,
  CAST(NULL AS TEXT)    AS reject_reason
WHERE 0;

WITH parsed AS (
  SELECT
    TransactionID,
    CustomerID,
    ProductID,
    StoreID,
    CASE
      WHEN TRIM(IFNULL(CampaignID,'')) = '' THEN NULL
      ELSE CAST(CampaignID AS INTEGER)
    END AS CampaignKey,
    SaleAmount,
    PercentDiscount,
    SaleFinal,
    CASE
      WHEN UPPER(TRIM(PaidWithPoints)) = 'YES' THEN 1
      WHEN UPPER(TRIM(PaidWithPoints)) = 'NO'  THEN 0
      ELSE NULL
    END AS PaidPointsInt,
    CAST(printf('%04d%02d%02d',
      CAST(substr(SaleDate, instr(SaleDate,'/') + 1 + instr(substr(SaleDate, instr(SaleDate,'/')+1),'/')) AS INT),
      CAST(substr(SaleDate, 1, instr(SaleDate,'/')-1) AS INT),
      CAST(substr(SaleDate, instr(SaleDate,'/')+1, instr(substr(SaleDate, instr(SaleDate,'/')+1),'/')-1) AS INT)
    ) AS INTEGER) AS date_key
  FROM staging_sales
),
invalids AS (
  SELECT p.*,
         CASE
           WHEN d.date_key IS NULL                  THEN 'date_key_not_found'
           WHEN c.customer_key IS NULL              THEN 'customer_key_not_found'
           WHEN pr.product_key IS NULL              THEN 'product_key_not_found'
           WHEN s.store_key IS NULL                 THEN 'store_key_not_found'
           WHEN p.CampaignKey IS NOT NULL
             AND cm.campaign_key IS NULL           THEN 'campaign_key_not_found'
           ELSE NULL
         END AS reject_reason
  FROM parsed p
  LEFT JOIN dim_date     d  ON p.date_key   = d.date_key
  LEFT JOIN dim_customer c  ON p.CustomerID = c.customer_key
  LEFT JOIN dim_product  pr ON p.ProductID  = pr.product_key
  LEFT JOIN dim_store    s  ON p.StoreID    = s.store_key
  LEFT JOIN dim_campaign cm ON p.CampaignKey = cm.campaign_key
),
valids AS ( SELECT * FROM invalids WHERE reject_reason IS NULL ),
bad    AS ( SELECT * FROM invalids WHERE reject_reason IS NOT NULL )

-- Insert valids
INSERT OR IGNORE INTO fact_sales (
  transaction_id, date_key, customer_key, product_key, store_key, campaign_key,
  sale_amount, percent_discount, sale_final, paid_with_points
)
SELECT
  TransactionID, date_key, CustomerID, ProductID, StoreID, CampaignKey,
  SaleAmount, PercentDiscount, SaleFinal, PaidPointsInt
FROM valids;

-- Archive rejects
INSERT INTO fact_sales_rejects (
  transaction_id, date_key, customer_key, product_key, store_key, campaign_key,
  sale_amount, percent_discount, sale_final, paid_with_points, reject_reason
)
SELECT
  TransactionID, date_key, CustomerID, ProductID, StoreID, CampaignKey,
  SaleAmount, PercentDiscount, SaleFinal, PaidPointsInt, reject_reason
FROM bad;

-- Quick summary
SELECT (SELECT COUNT(*) FROM fact_sales)         AS fact_rows_loaded,
       (SELECT COUNT(*) FROM fact_sales_rejects) AS rows_rejected;
