-- Which 5 products generate the highest revenue
SELECT
    b.product_name,
    SUM(a.sales) AS total_revenue
FROM gold.fact_sales a
LEFT JOIN gold.dim_products b
ON a.product_key = b.product_key
GROUP BY b.product_name
ORDER BY total_revenue DESC
LIMIT 5;

-- Using window functions
SELECT 
	t.product_name,
	t.total_revenue
FROM (
	SELECT
	    b.product_name,
	    SUM(a.sales) AS total_revenue,
		RANK() OVER (ORDER BY SUM(a.sales) DESC) AS rank_products
	FROM gold.fact_sales a
	LEFT JOIN gold.dim_products b
	ON a.product_key = b.product_key
	GROUP BY b.product_name
) AS t
WHERE rank_products <= 5;

-- Which 5 products generate the lowest revenue
SELECT
    b.product_name,
    SUM(a.sales) AS total_revenue
FROM gold.fact_sales a
LEFT JOIN gold.dim_products b
ON a.product_key = b.product_key
GROUP BY b.product_name
ORDER BY total_revenue
LIMIT 5;

-- Find the top 10 customers who have generated the highest revenue
SELECT
	b.customer_key,
	b.first_name,
	b.last_name,
	SUM(a.sales) AS total_revenue
FROM gold.fact_sales AS a
LEFT JOIN gold.dim_customers AS b
ON a.customer_key = b.customer_key
GROUP BY b.customer_key, b.first_name, b.last_name
ORDER BY total_revenue DESC
LIMIT 10;

-- The 3 customers with the fewest orders placed.
SELECT
	b.customer_key,
	b.first_name,
	b.last_name,
	COUNT(DISTINCT a.order_number) AS total_orders
FROM gold.fact_sales AS a
LEFT JOIN gold.dim_customers AS b
ON a.customer_key = b.customer_key
GROUP BY b.customer_key, b.first_name, b.last_name
ORDER BY total_orders
LIMIT 3;
