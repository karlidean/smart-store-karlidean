PRAGMA foreign_keys = ON;

-- Customer
INSERT OR IGNORE INTO dim_customer (
  customer_key, customer_name, region, join_date,
  member_points, member_status, preferred_contact
)
SELECT
  CustomerID,
  NULLIF(Name, ''),
  NULLIF(Region, ''),
  -- Convert 'M/D/YYYY' -> 'YYYY-MM-DD' using string ops
  CASE
    WHEN TRIM(IFNULL(JoinDate,'')) = '' THEN NULL
    ELSE printf('%04d-%02d-%02d',
      CAST(substr(JoinDate, instr(JoinDate,'/') + 1 + instr(substr(JoinDate, instr(JoinDate,'/')+1),'/')) AS INT), -- year
      CAST(substr(JoinDate, 1, instr(JoinDate,'/')-1) AS INT),                                                   -- month
      CAST(substr(JoinDate, instr(JoinDate,'/')+1, instr(substr(JoinDate, instr(JoinDate,'/')+1),'/')-1) AS INT) -- day
    )
  END,
  MemberPoints,
  NULLIF(MemberStatus, ''),
  NULLIF(PreferredContact, '')
FROM staging_customers;

-- Product
INSERT OR IGNORE INTO dim_product (
  product_key, product_name, category, unit_price,
  year_released, month_purchased, ordering_sku, supplier_name
)
SELECT
  ProductID,
  NULLIF(ProductName,''),
  NULLIF(Category,''),
  UnitPrice,
  YearReleased,
  NULLIF(MonthPurchased,''),
  NULLIF(OrderingSKU,''),
  NULLIF(SupplierName,'')
FROM staging_products;

-- Store (distinct from sales)
INSERT OR IGNORE INTO dim_store (store_key, store_name, region)
SELECT DISTINCT
  StoreID, NULL, NULL
FROM staging_sales
WHERE StoreID IS NOT NULL;

-- Campaign (distinct from sales, integerize)
WITH cleaned AS (
  SELECT DISTINCT NULLIF(TRIM(CampaignID), '') AS cid
  FROM staging_sales
)
INSERT OR IGNORE INTO dim_campaign (campaign_key, campaign_name)
SELECT
  CAST(cid AS INTEGER) AS campaign_key,  -- SQLite CAST to INTEGER truncates decimals
  NULL
FROM cleaned
WHERE cid IS NOT NULL;

-- Date (distinct parsed from SaleDate 'M/D/YYYY')
WITH raw AS (
  SELECT DISTINCT TRIM(SaleDate) AS s
  FROM staging_sales
  WHERE TRIM(IFNULL(SaleDate,'')) <> ''
),
parts AS (
  SELECT
    s,
    instr(s,'/') AS p1,
    instr(substr(s, instr(s,'/')+1), '/') AS p2rel
  FROM raw
),
pieces AS (
  SELECT
    s,
    CAST(substr(s,1,p1-1) AS INT) AS m,
    CAST(substr(s,p1+1,p2rel-1) AS INT) AS d,
    CAST(substr(s, p1+1+p2rel) AS INT) AS y
  FROM parts
),
formatted AS (
  SELECT
    s,
    printf('%04d-%02d-%02d', y, m, d) AS full_date_iso,
    CAST(printf('%04d%02d%02d', y, m, d) AS INTEGER) AS date_key,
    d AS day,
    m AS month,
    y AS year,
    (( (m - 1) / 3) + 1) AS quarter,
    strftime('%w', printf('%04d-%02d-%02d', y, m, d)) AS weekday_num
  FROM pieces
)
INSERT OR IGNORE INTO dim_date (date_key, full_date, day, month, year, quarter, weekday)
SELECT
  date_key,
  full_date_iso,
  day,
  month,
  year,
  quarter,
  CASE weekday_num
    WHEN '0' THEN 'Sunday'
    WHEN '1' THEN 'Monday'
    WHEN '2' THEN 'Tuesday'
    WHEN '3' THEN 'Wednesday'
    WHEN '4' THEN 'Thursday'
    WHEN '5' THEN 'Friday'
    WHEN '6' THEN 'Saturday'
  END
FROM formatted;
