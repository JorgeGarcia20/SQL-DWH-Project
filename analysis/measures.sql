-- DATABASE EXPLORATION

-- With the following query we can explore all the existing tables inside a database.
SELECT * FROM INFORMATION_SCHEMA.TABLES;

-- With the following query we can explore all the columns existin in the diferent tables.
SELECT * FROM INFORMATION_SCHEMA.COLUMNS;


-- DIMENSION EXPLORATION

-- Explore all the countries our customers come
SELECT DISTINCT country FROM gold.dim_customers;

-- Explore all categories "The major division"
SELECT DISTINCT category, subcategory, product_name 
FROM gold.dim_products
ORDER BY 1, 2, 3;

-- DATE EXPLORATION
-- Find the date of the first and last order
-- How many years of sales are available

SELECT 
	MIN(order_date) AS first_order_date,
	MAX(order_date) AS last_order_date,
	-- AGE(MAX(order_date), MIN(order_date)) AS order_range_time
	EXTRACT(YEAR FROM AGE(MAX(order_date), MIN(order_date))) AS order_range_years,
	EXTRACT(MONTH FROM AGE(MIN(order_date), MAX(order_date))) AS order_range_months
FROM gold.fact_sales;

-- Find the youngest and oldest customer
SELECT 
	MIN(birthdate) AS oldest_birthdate,
	EXTRACT(YEAR FROM AGE(CURRENT_DATE, MIN(birthdate))) AS oldest_age,
	-- AGE(CURRENT_DATE, MIN(birthdate)) AS olderst_age,
	MAX(birthdate) AS youngest_birthdate,
	EXTRACT(YEAR FROM AGE(CURRENT_DATE, MAX(birthdate))) AS youngest_age
	-- AGE(CURRENT_DATE, MAX(birthdate)) AS youngest_age 
FROM gold.dim_customers;

-- MEASURE EXPLORATION
SELECT * FROM gold.fact_sales;
SELECT * FROM gold.dim_products;

-- Find the Total Sales
SELECT SUM(sales) AS total_sales FROM gold.fact_sales;

-- Find how many items are sold
SELECT SUM(quantity) AS total_quantity FROM gold.fact_sales;

-- Find the average selling price
SELECT AVG(price) AS avg_price FROM gold.fact_sales;

-- Find the total number of orders
SELECT COUNT(order_number) FROM gold.fact_sales;
SELECT COUNT(DISTINCT order_number) FROM gold.fact_sales;

-- Find the total number of products
SELECT COUNT(product_key) AS total_products FROM gold.dim_products;

-- Find the total number of customers
SELECT COUNT(customer_key) AS total_customers FROM gold.dim_customers;
SELECT COUNT(DISTINCT customer_key) AS total_customers FROM gold.dim_customers;

-- Find the total number of customers that has placed an order
SELECT COUNT(DISTINCT customer_key) AS total_customers FROM gold.fact_sales; 


-- REPORT
-- Generate a report that shows all the key metrics of the business
SELECT 'Total Sales' AS measuare_name, SUM(sales) AS measuare_value FROM gold.fact_sales
UNION ALL
SELECT 'Total Quantity', SUM(quantity) FROM gold.fact_sales
UNION ALL
SELECT 'Average Price',  ROUND(AVG(sales), 2) FROM gold.fact_sales
UNION ALL
SELECT 'Total Nr. Orders', COUNT(DISTINCT order_number) FROM gold.fact_sales
UNION ALL
SELECT 'Total Nr. Products', COUNT(product_name) FROM gold.dim_products
UNION ALL
SELECT 'Total Nr. Customers', COUNT(customer_key) FROM gold.dim_customers

