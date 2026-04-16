-- MAGNITUDE

-- Find total customers by countries
SELECT
	country,
	COUNT(customer_key) AS total_customers
FROM gold.dim_customers
GROUP BY country
ORDER BY total_customers DESC;

-- Find total customers by gender
SELECT
	gender,
	COUNT(customer_key) AS total_customers
FROM gold.dim_customers
GROUP BY gender
ORDER BY total_customers DESC;

-- Find total products by category
SELECT
	category,
	COUNT(product_key) AS total_products
FROM gold.dim_products
GROUP BY category
ORDER BY total_products DESC;

-- What is the average costs in each category?
SELECT
	category,
	AVG(cost) AS avg_costs
FROM gold.dim_products
GROUP BY category
ORDER BY avg_costs DESC;

-- What is the total revenue generated for each category

SELECT 
	b.category,
	SUM(a.sales) AS total_revenue
FROM gold.fact_sales a
LEFT JOIN gold.dim_products b
ON a.product_key = b.product_key
GROUP BY b.category
ORDER BY total_revenue DESC;

-- What is the total revenue generated for each customer
SELECT
	b.customer_key,
	b.first_name,
	b.last_name,
	SUM(a.sales) AS total_revenue
FROM gold.fact_sales a 
LEFT JOIN gold.dim_customers b
ON a.customer_key = b.customer_key
GROUP BY b.customer_key, b.first_name, b.last_name
ORDER BY total_revenue DESC;

-- What is the distribution of sold items across countries?
SELECT
	b.country,
	SUM(a.quantity) AS total_sold_items
FROM gold.fact_sales a 
LEFT JOIN gold.dim_customers b
ON a.customer_key = b.customer_key
GROUP BY b.country
ORDER BY total_sold_items DESC;