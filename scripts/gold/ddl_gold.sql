/*
=========================================================
DDL for Gold Layer
=========================================================
Script purpose:
    This script defines the Data Definition Language (DDL) for the gold layer of the data warehouse.
    It creates three views: dim_customers, dim_products, and fact_sales.
    The views are created by joining relevant tables from the silver layer to create a star schema structure for analytics.

Tables involved:
    - gold.dim_customers: A dimension table containing customer information, created by joining silver.crm_cust_info, silver.erp_cust_az12, and silver.erp_loc_a101.
    - gold.dim_products: A dimension table containing product information, created by joining silver.crm_prd_info and silver.erp_px_cat_g1v2.
    - gold.fact_sales: A fact table containing sales details, created by joining gold.dim_customers, gold.dim_products, and silver.crm_sales_details.

*/

-- ========================================================
-- gold.dim_customers 
-- This view is created by joining the silver.crm_cust_info,
-- silver.erp_cust_az12 and silver.erp_loc_a101 tables.
-- ========================================================

DROP VIEW IF EXISTS gold.dim_customers CASCADE;
CREATE VIEW gold.dim_customers AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY cci.cst_id) AS customer_key,
    cci.cst_id AS custumer_id,
    cci.cst_key AS custumer_number,
    cci.cst_first_name AS first_name,
    cci.cst_last_name AS last_name,
    ela.cntry AS country,
    cci.cst_maritial_status AS maritial_status,
    CASE WHEN cci.cst_gnder != 'n/a' THEN cci.cst_gnder -- CRM is the Master for gender info
        ELSE COALESCE(eca.gen, 'n/a')
    END AS gender,
    eca.bdate AS birthdate,
    cci.cst_create_date AS create_date
FROM silver.crm_cust_info cci
LEFT JOIN silver.erp_cust_az12 eca 
ON cci.cst_key = eca.cid
LEFT JOIN silver.erp_loc_a101 ela 
ON cci.cst_key = ela.cid;

-- ========================================================
-- gold.dim_products
-- This view is created by joining the silver.crm_prd_info table.
-- ========================================================
DROP VIEW IF EXISTS gold.dim_products CASCADE;
CREATE VIEW gold.dim_products AS
SELECT
    ROW_NUMBER() OVER (ORDER BY cpi.prd_start_dt, cpi.prd_key) AS product_key,
    cpi.prd_id AS product_id,
    cpi.prd_key AS product_number,
    cpi.prd_nm AS product_name,
    cpi.cat_id AS category_id,
    epc.cat AS category,
    epc.subcat AS subcategory,
    epc.maintence,
    cpi.prd_cost AS cost,
    cpi.prd_line AS product_line,
    cpi.prd_start_dt AS start_date
FROM silver.crm_prd_info cpi
LEFT JOIN silver.erp_px_cat_g1v2 epc ON cpi.cat_id = epc.id
WHERE cpi.prd_end_dt IS NULL;

-- ========================================================
-- gold.fact_sales
-- This view is created by joining the gold.dim_customers, 
-- gold.dim_products and silver.crm_sales_details tables.
-- ========================================================
DROP VIEW IF EXISTS gold.fact_sales CASCADE;
CREATE VIEW gold.fact_sales AS
SELECT
    csd.sls_ord_num AS order_number,
    dim_prds.product_key,
    dim_cust.customer_key,
    csd.sls_order_dt AS order_date,
    csd.sls_ship_dt AS shipping_date,
    csd.sls_due_dt AS due_date,
    csd.sls_sales AS sales,
    csd.sls_quantity AS quantity,
    csd.sls_price AS price
FROM silver.crm_sales_details csd
LEFT JOIN gold.dim_products dim_prds 
ON csd.sls_prd_key = dim_prds.product_number
LEFT JOIN gold.dim_customers dim_cust
ON csd.sls_cust_id = dim_cust.custumer_id;

