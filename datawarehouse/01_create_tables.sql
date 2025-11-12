PRAGMA foreign_keys = ON;

-- Drop in dependency order
DROP TABLE IF EXISTS fact_sales;
DROP TABLE IF EXISTS dim_date;
DROP TABLE IF EXISTS dim_campaign;
DROP TABLE IF EXISTS dim_store;
DROP TABLE IF EXISTS dim_product;
DROP TABLE IF EXISTS dim_customer;

-- Dimension: Customer
CREATE TABLE dim_customer (
  customer_key       INTEGER PRIMARY KEY,  -- = CustomerID
  customer_name      TEXT,
  region             TEXT,
  join_date          TEXT,                 -- store ISO 'YYYY-MM-DD' as TEXT
  member_points      INTEGER,
  member_status      TEXT,
  preferred_contact  TEXT
);

-- Dimension: Product
CREATE TABLE dim_product (
  product_key     INTEGER PRIMARY KEY,     -- = ProductID
  product_name    TEXT,
  category        TEXT,
  unit_price      REAL,
  year_released   INTEGER,
  month_purchased TEXT,
  ordering_sku    TEXT,
  supplier_name   TEXT
);

-- Dimension: Store (from distinct StoreID)
CREATE TABLE dim_store (
  store_key   INTEGER PRIMARY KEY,         -- = StoreID
  store_name  TEXT,
  region      TEXT
);

-- Dimension: Campaign (from distinct CampaignID)
CREATE TABLE dim_campaign (
  campaign_key  INTEGER PRIMARY KEY,       -- integerized CampaignID
  campaign_name TEXT
);

-- Dimension: Date (built from Sales "M/D/YYYY" strings)
CREATE TABLE dim_date (
  date_key   INTEGER PRIMARY KEY,          -- YYYYMMDD
  full_date  TEXT NOT NULL,                -- 'YYYY-MM-DD'
  day        INTEGER,
  month      INTEGER,
  year       INTEGER,
  quarter    INTEGER,
  weekday    TEXT
);

-- Fact: Sales
CREATE TABLE fact_sales (
  transaction_id    INTEGER PRIMARY KEY,
  date_key          INTEGER NOT NULL REFERENCES dim_date(date_key),
  customer_key      INTEGER NOT NULL REFERENCES dim_customer(customer_key),
  product_key       INTEGER NOT NULL REFERENCES dim_product(product_key),
  store_key         INTEGER     REFERENCES dim_store(store_key),
  campaign_key      INTEGER     REFERENCES dim_campaign(campaign_key),
  sale_amount       REAL,
  percent_discount  REAL,
  sale_final        REAL,
  paid_with_points  INTEGER     -- 1 = True, 0 = False, NULL = unknown
);

-- Helpful indexes
CREATE INDEX IF NOT EXISTS idx_fact_sales_date    ON fact_sales(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_cust    ON fact_sales(customer_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_prod    ON fact_sales(product_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_store   ON fact_sales(store_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_campaign ON fact_sales(campaign_key);
