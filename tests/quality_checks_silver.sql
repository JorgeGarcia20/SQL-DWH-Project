/**
=========================================================
Quality Checks for Silver Layer
=========================================================

Script purpose:
    This script performs a series of quality checks on the data in the silver layer
    to ensure data integrity, consistency, and accuracy. The checks include:
    1. Checking for null or duplicate values in primary key fields of silver tables.
    2. Checking for unwanted spaces in text fields.
    3. Checking for null or negative values in numeric fields.
    4. Data standardization and consistency checks for categorical fields.
    5. Checking for invalid date orders and out-of-range dates.

Usage notes:
    - Run these ckecks after loading data into the Silver layer.
*/

-- ========================================================
-- Checking silver.crm_prd_info
-- Check for null or duplicate values in primary key fields of silver tables.
-- Expectation: No records should be returned.
-- =======================================================
SELECT prd_id, COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1
    OR prd_id IS NULL;

-- ========================================================
-- Check for unwanted spaces 
-- Expectation: No records should be returned.
-- ========================================================
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm); 

-- ========================================================
-- Check for null or negative values in cost
-- Expectation: No records should be returned.
-- ========================================================
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost IS NULL
    OR prd_cost <= 0;

-- ========================================================
-- Data Standarization & Consistency
-- ========================================================
SELECT DISTINCT prd_line
FROM silver.crm_prd_info;

-- ========================================================
-- Check for invalid date orders (start date > end date)
-- Expectation: No records should be returned.
-- =======================================================
SELECT *
FROM silver.crm_prd_info
WHERE prd_start_dt > prd_end_dt;

-- ========================================================
-- Checking silver.crm_sales_details
-- Check for invalid dates 
-- Expectation: No records should be returned.
-- ========================================================
SELECT NULLIF(sls_due_dt, 0) AS sls_due_dt
FROM bronze.crm_sales_datils
WHERE sls_due_dt <= 0
    OR LENGTH(sls_due_dt::TEXT) != 8
    OR sls_due_dt > 20501231
    OR sls_due_dt < 19000101;



-- ========================================================
-- Check for invalid dated orders (Date order > Shipping/Due date)
-- Expectation: No records should be returned.
-- ========================================================

SELECT *
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt
    OR sls_order_dt > sls_due_dt;

-- ========================================================
-- Check data consistency: Sales = Quantity * Price
-- Expectation: No records should be returned.
-- =======================================================
SELECT DISTINCT
    sls_sales, 
    sls_quantity, 
    sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
    OR sls_sales IS NULL
    OR sls_quantity IS NULL
    OR sls_price IS NULL
    OR sls_sales <= 0
    OR sls_quantity <= 0
    OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;

-- ========================================================
-- Checking silver.erp_cust_az12
-- Identify out of range dates
-- Expectation: birthdates between 1924 and today
-- =======================================================
SELECT bdate
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' 
    OR bdate > CURRENT_DATE;


-- ========================================================
-- Checking silver.erp_cust_az12
-- Data standarization & consistency check for gender field.
-- Expectation: Should return distinct list of standarized gender values.
-- for this dataset, it's expected to see 'm', 'f' and 'n/a' as the distinct values.
-- ========================================================

SELECT DISTINCT gen
FROM silver.erp_cust_az12


-- ========================================================
-- Checking silver.erp_loc_a101
-- Data standarization & consistency check for country field.
-- Expectation: Should return distinct list of standarized country names.
-- ========================================================
SELECT DISTINCT cntry
FROM silver.erp_loc_a101
ORDER BY cntry;


-- ========================================================
-- Checking silver.erp_px_cat_g1v2
-- Check for unwanted spaces 
-- Expectation: No records should be returned.
-- ========================================================

SELECT * 
FROM silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat)
    OR subcat != TRIM(subcat)
    OR maintence != TRIM(maintence)

-- ========================================================
-- Data standarization & consistency check for maintence field.
-- Expectation: Should return distinct list of standarized maintence values.
-- ========================================================
SELECT DISTINCT maintence
FROM silver.erp_px_cat_g1v2;