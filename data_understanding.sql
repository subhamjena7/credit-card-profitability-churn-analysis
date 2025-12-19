USE credit_card_bfsi;

SET SESSION sql_mode = '';
SET FOREIGN_KEY_CHECKS = 0;

CREATE TABLE customers (
    customer_id BIGINT PRIMARY KEY,
    age INT,
    gender VARCHAR(10),
    city VARCHAR(50),
    account_tenure_months INT
);
LOAD DATA LOCAL INFILE 'C:/Users/KIIT/Desktop/Professional/Projects/BFSI Project/Datasets/customers.csv'
INTO TABLE customers
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(
  customer_id,
  age,
  gender,
  city,
  account_tenure_months
);

CREATE TABLE credit_profiles (
    credit_profile_id BIGINT PRIMARY KEY,
    customer_id BIGINT,
    credit_limit DECIMAL(12,2),
    credit_score INT NULL,
    utilization_ratio DECIMAL(5,2),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);
LOAD DATA LOCAL INFILE 'C:/Users/KIIT/Desktop/Professional/Projects/BFSI Project/Datasets/credit_profiles.csv'
INTO TABLE credit_profiles
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(
  credit_profile_id,
  customer_id,
  credit_limit,
  @credit_score,
  utilization_ratio
)
SET
  credit_score = NULLIF(@credit_score, '');

CREATE TABLE transactions (
    transaction_id BIGINT PRIMARY KEY,
    customer_id BIGINT,
    transaction_date DATETIME(6),
    transaction_category VARCHAR(50),
    transaction_amount DECIMAL(14,2),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);
ALTER TABLE transactions
MODIFY transaction_amount DECIMAL(14,2);

ALTER TABLE transactions
MODIFY transaction_amount DECIMAL(18,2);

TRUNCATE TABLE transactions;

LOAD DATA LOCAL INFILE 'C:/Users/KIIT/Desktop/Professional/Projects/BFSI Project/Datasets/transactions.csv'
INTO TABLE transactions
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(
  transaction_id,
  customer_id,
  @transaction_date,
  transaction_category,
  @raw_amount
)
SET
  transaction_date = STR_TO_DATE(
      @transaction_date,
      '%Y-%m-%d %H:%i:%s.%f'
  ),
  transaction_amount = CAST(
      REPLACE(TRIM(@raw_amount), ',', '') AS DECIMAL(18,2)
  );

CREATE TABLE repayments (
    repayment_id BIGINT PRIMARY KEY,
    customer_id BIGINT,
    payment_date DATE,
    payment_amount DECIMAL(14,2),
    payment_delay_days INT,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);
ALTER TABLE repayments
MODIFY payment_date DATETIME(6);

TRUNCATE TABLE repayments;

LOAD DATA LOCAL INFILE 'C:/Users/KIIT/Desktop/Professional/Projects/BFSI Project/Datasets/repayments.csv'
INTO TABLE repayments
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(
  repayment_id,
  customer_id,
  @payment_date,
  payment_amount,
  payment_delay_days
)
SET
  payment_date = STR_TO_DATE(
      @payment_date,
      '%Y-%m-%d %H:%i:%s.%f'
  );

---------------------------------------------------------------------------------------

SHOW TABLES;

---------------------------------------------------------------------------------------

-- Understand Table Schema & Data Types
DESCRIBE customers;
DESCRIBE credit_profiles;
DESCRIBE transactions;
DESCRIBE repayments;

-- Validate Primary Keys (PK)
-- customers table
SELECT 
    COUNT(*) AS total_rows,
    COUNT(DISTINCT customer_id) AS distinct_customers
FROM customers;

SELECT *
FROM customers
WHERE customer_id IS NULL;

-- credit_profiles table
SELECT 
    COUNT(*) AS total_rows,
    COUNT(DISTINCT credit_profile_id) AS distinct_customers
FROM credit_profiles;

SELECT *
FROM credit_profiles
WHERE credit_profile_id IS NULL;

-- transactions table
SELECT 
    COUNT(*) AS total_rows,
    COUNT(DISTINCT transaction_id) AS distinct_customers
FROM transactions;

SELECT *
FROM transactions
WHERE transaction_id IS NULL;

-- repayments table
SELECT 
    COUNT(*) AS total_rows,
    COUNT(DISTINCT repayment_id) AS distinct_customers
FROM repayments;

SELECT *
FROM repayments
WHERE repayment_id IS NULL;

-------------------------------------------------------------------

-- Validate Foreign Key (FK) Relationships
SELECT
    'credit_profiles' AS table_name,
    COUNT(*) AS orphan_records
FROM credit_profiles cp
LEFT JOIN customers c
    ON cp.customer_id = c.customer_id
WHERE c.customer_id IS NULL

UNION ALL

SELECT
    'transactions',
    COUNT(*)
FROM transactions t
LEFT JOIN customers c
    ON t.customer_id = c.customer_id
WHERE c.customer_id IS NULL

UNION ALL

SELECT
    'repayments',
    COUNT(*)
FROM repayments r
LEFT JOIN customers c
    ON r.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

---------------------------------------------------------
-- Understand Data Volume & Grain
SELECT COUNT(*) FROM customers;
SELECT COUNT(*) FROM credit_profiles;
SELECT COUNT(*) FROM transactions;
SELECT COUNT(*) FROM repayments;

---------------------------------------------------------
-- Initial Null & Missing Value Assessment
SELECT 'customers' AS table_name,
       COUNT(*) AS total_rows,
       SUM(age IS NULL OR gender IS NULL OR city IS NULL OR account_tenure_months IS NULL) AS rows_with_nulls
FROM customers

UNION ALL

SELECT 'credit_profiles',
       COUNT(*),
       SUM(credit_limit IS NULL OR credit_score IS NULL OR utilization_ratio IS NULL)
FROM credit_profiles

UNION ALL

SELECT 'transactions',
       COUNT(*),
       SUM(transaction_date IS NULL OR transaction_category IS NULL OR transaction_amount IS NULL)
FROM transactions

UNION ALL

SELECT 'repayments',
       COUNT(*),
       SUM(payment_date IS NULL OR payment_amount IS NULL OR payment_delay_days IS NULL)
FROM repayments;

--------------------------------------------------------------------------------
-- Check for Duplicate Business Records
SELECT
    'customers' AS table_name,
    COUNT(*) - COUNT(DISTINCT customer_id) AS duplicate_rows
FROM customers
-- Two customer rows with the same demographics + tenure indicate a customer master issue, even if PKs differ.
UNION ALL

SELECT
    'credit_profiles',
    COUNT(*) - COUNT(DISTINCT customer_id)
FROM credit_profiles
-- Each customer should ideally have one active credit profile.
UNION ALL

SELECT
    'transactions',
    COUNT(*) - COUNT(DISTINCT CONCAT(customer_id, transaction_date, transaction_category, transaction_amount))
FROM transactions
-- A transaction is duplicated if: Same customer, Same timestamp, Same category, Same amount
UNION ALL

SELECT
    'repayments',
    COUNT(*) - COUNT(DISTINCT CONCAT(customer_id, payment_date, payment_amount))
FROM repayments;
-- A repayment is duplicated if: Same customer, Same payment date, Same amount


---------------------------------------------------------------------------
-- Validate Transaction Category Consistency
SELECT
    COUNT(DISTINCT transaction_category) AS raw_category_count,
    COUNT(DISTINCT LOWER(TRIM(transaction_category))) AS normalized_category_count
FROM transactions;

---------------------------------------------------------------------------
-- Identify Potential Outliers (High-Level)

-- Detect unusually high or low spending transactions that may skew profitability.
SELECT
    MIN(transaction_amount) AS min_amount,
    MAX(transaction_amount) AS max_amount,
    AVG(transaction_amount) AS avg_amount,
    STDDEV(transaction_amount) AS stddev_amount
FROM transactions;

-- Visually inspect extreme spend values.
SELECT
    transaction_id,
    customer_id,
    transaction_amount,
    transaction_date
FROM transactions
ORDER BY transaction_amount DESC
LIMIT 20;

-- Identify customers whose total spend is abnormally high.
SELECT
    customer_id,
    SUM(transaction_amount) AS total_spend
FROM transactions
GROUP BY customer_id
ORDER BY total_spend DESC
LIMIT 20;

-- Detect customers with unusually high transaction counts.
SELECT
    customer_id,
    COUNT(*) AS transaction_count
FROM transactions
GROUP BY customer_id
ORDER BY transaction_count DESC
LIMIT 20;

-- Find customers over-utilizing credit limits.
SELECT
    MIN(utilization_ratio) AS min_utilization,
    MAX(utilization_ratio) AS max_utilization,
    AVG(utilization_ratio) AS avg_utilization
FROM credit_profiles;

-- Detect customers with exceptionally high credit exposure.
SELECT
    customer_id,
    credit_limit
FROM credit_profiles
ORDER BY credit_limit DESC
LIMIT 20;

-- Identify severe payment delays.
SELECT
    MIN(payment_delay_days) AS min_delay,
    MAX(payment_delay_days) AS max_delay,
    AVG(payment_delay_days) AS avg_delay
FROM repayments;

-- Detect customers showing repeated delinquent behavior.
SELECT
    customer_id,
    COUNT(*) AS delayed_payments
FROM repayments
WHERE payment_delay_days > 0
GROUP BY customer_id
ORDER BY delayed_payments DESC
LIMIT 20;

-------------------------------------------------
-- Define Core Business Metrics (SQL View)
CREATE VIEW customer_base_metrics AS
SELECT
    c.customer_id,
    c.age,
    c.gender,
    c.city,
    c.account_tenure_months,
    cp.credit_limit,
    cp.credit_score,
    cp.utilization_ratio,
    COUNT(t.transaction_id) AS txn_count,
    SUM(t.transaction_amount) AS total_spend,
    AVG(t.transaction_amount) AS avg_txn_value,
    SUM(r.payment_amount) AS total_payments,
    AVG(r.payment_delay_days) AS avg_delay_days
FROM customers c
LEFT JOIN credit_profiles cp ON c.customer_id = cp.customer_id
LEFT JOIN transactions t ON c.customer_id = t.customer_id
LEFT JOIN repayments r ON c.customer_id = r.customer_id
GROUP BY c.customer_id;

--------------------------------------------------------------------------------
-- Data Extraction
SELECT
    customer_id,
    age,
    gender,
    city,
    account_tenure_months
FROM customers;

SELECT
    credit_profile_id,
    customer_id,
    credit_limit,
    credit_score,
    utilization_ratio
FROM credit_profiles;

SELECT
    transaction_id,
    customer_id,
    transaction_date,
    transaction_category,
    transaction_amount
FROM transactions;

SELECT
    repayment_id,
    customer_id,
    payment_date,
    payment_amount,
    payment_delay_days
FROM repayments;

SELECT
    c.customer_id,
    c.age,
    c.gender,
    c.city,
    c.account_tenure_months,

    cp.credit_limit,
    cp.credit_score,
    cp.utilization_ratio,

    COUNT(DISTINCT t.transaction_id) AS total_transactions,
    SUM(t.transaction_amount) AS total_spend,
    AVG(t.transaction_amount) AS avg_transaction_value,
    MAX(t.transaction_amount) AS max_transaction_value,

    COUNT(DISTINCT r.repayment_id) AS total_repayments,
    SUM(r.payment_amount) AS total_repaid,
    AVG(r.payment_delay_days) AS avg_payment_delay,
    MAX(r.payment_delay_days) AS max_payment_delay

FROM customers c
LEFT JOIN credit_profiles cp
    ON c.customer_id = cp.customer_id
LEFT JOIN transactions t
    ON c.customer_id = t.customer_id
LEFT JOIN repayments r
    ON c.customer_id = r.customer_id
GROUP BY
    c.customer_id,
    c.age,
    c.gender,
    c.city,
    c.account_tenure_months,
    cp.credit_limit,
    cp.credit_score,
    cp.utilization_ratio;

SELECT
    customer_id,
    DATE_FORMAT(transaction_date, '%Y-%m') AS ym,
    SUM(transaction_amount) AS monthly_spend,
    COUNT(*) AS monthly_transactions
FROM transactions
GROUP BY
    customer_id,
    DATE_FORMAT(transaction_date, '%Y-%m');

SELECT
    customer_id,
    LOWER(TRIM(transaction_category)) AS transaction_category,
    SUM(transaction_amount) AS category_spend,
    COUNT(*) AS category_transactions
FROM transactions
GROUP BY
    customer_id,
    LOWER(TRIM(transaction_category));




























