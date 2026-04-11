-- ========================================================
-- Checking 'gold.dim_customers'
-- Check for uniqueness of the customer_key in gold.dim_customers table.
-- Results should be empty, if there are any records returned, it means 
-- there are duplicate customer_key in dimension table which will cause
-- issue in data model.
-- ========================================================

SELECT customer_key, COUNT(*)
FROM gold.dim_customers
GROUP BY customer_key 
HAVING COUNT(*) > 1;

-- ========================================================
-- Checking 'gold.dim_products'
-- Check for uniqueness of the product_key in gold.dim_products table.
-- Results should be empty, if there are any records returned, it means 
-- there are duplicate product_key in dimension table which will cause
-- issue in data model.
-- ========================================================

SELECT product_key, COUNT(*)
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;

-- ========================================================
-- Checking 'gold.fact_sales'
-- Check the data model conectivity between fact and dimension tables.
-- Results should be empty, if there are any records returned, it means 
-- there are missing records in dimension tables for the fact table.
-- ========================================================

SELECT *
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c 
ON f.customer_key = c.customer_key
LEFT JOIN gold.dim_products p 
ON f.product_key = p.product_key
WHERE p.product_key IS NULL OR c.customer_key IS NULL;