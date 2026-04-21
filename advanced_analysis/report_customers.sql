/*
 * ========================================================================== 
 * Customer Report
 * ==========================================================================
 * Purpose:
 * 	- This report consolidates key customer metrics and behaviors.
 * 
 * Highlights:
 * 	1. Gathers essencial fields such as names, ages, and transaction details.
 * 	2. Segments customers into categories (VIP, Regular, New) and age groups.
 * 	3. Aggregates customer-level metrics:
 * 		- total orders
 * 		- total sales
 * 		- total quantity purchused
 * 		- total products
 * 		- lifespan (in months)
 * 	4. Calculates valuable KPIs:
 * 		- recency (months since last order)
 * 		- average order value
 * 		- average monthly spend 
 * ==========================================================================
 **/

CREATE VIEW gold.report_customers AS 
WITH base_query AS (
/* ----------------------------------------------------------------------------
 * 1. Base Query: Retrives core columns from tables
 * ----------------------------------------------------------------------------*/
	SELECT
		f.order_number,
		f.product_key,
		f.order_date,
		f.sales,
		f.quantity,
		c.customer_key,
		c.custumer_number,
		CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
		EXTRACT(YEAR FROM AGE(CURRENT_TIMESTAMP, c.birthdate)) AS customer_age
	FROM gold.fact_sales f
	LEFT JOIN gold.dim_customers c
	ON f.product_key = c.customer_key
),
customer_aggregation AS (
/* ----------------------------------------------------------------------------
 * 2. Customer Aggregations: Summarizes key metrics at the customer lavel
 * ----------------------------------------------------------------------------*/
SELECT
	customer_key,
	custumer_number,
	customer_name,
	customer_age,
	count(DISTINCT order_number) AS total_orders,
	sum(sales) AS total_sales,
	sum(quantity) AS total_quantity,
	count(DISTINCT product_key) AS total_products,
	max(order_date) AS last_order_date,
	(EXTRACT(YEAR FROM AGE(MAX(order_date), MIN(order_date))) * 12) + EXTRACT(MONTH FROM AGE(MAX(order_date), MIN(order_date))) AS lifespan
FROM base_query
GROUP BY
	customer_key,
	custumer_number,
	customer_name,
	customer_age
)
SELECT
	customer_key,
	custumer_number,
	customer_name,
	customer_age,
	CASE
		WHEN customer_age < 20 THEN 'Under 20'
		WHEN customer_age BETWEEN 20 AND 29 THEN '20-29'
		WHEN customer_age BETWEEN 30 AND 39 THEN '30-39'
		WHEN customer_age BETWEEN 40 AND 49 THEN '40-49'
		ELSE '50 and above'
	END AS age_group,
	CASE 
		WHEN lifespan >= 12 AND total_sales > 5000 THEN 'VIP'
		WHEN lifespan >= 12 AND total_sales <= 5000 THEN 'Regular'
		ELSE 'New'
	END AS customer_segment,
	last_order_date,
	(EXTRACT(YEAR FROM AGE(CURRENT_DATE, last_order_date)) * 12) + EXTRACT(MONTH FROM AGE(CURRENT_DATE, last_order_date)) AS rencency,
	total_orders,
	total_sales,
	total_quantity,
	total_products,
	lifespan,
	-- Compute average order value (AVO)
	CASE WHEN total_sales = 0 THEN 0
		ELSE total_sales / total_orders 
	END AS avg_order_value,
	-- Compute average monthly spend
	CASE WHEN lifespan = 0 THEN total_sales
		ELSE ROUND((total_sales / lifespan), 2)
	END AS avg_monthly_spend	
FROM customer_aggregation;