/**
* DATA SEGMENTATION
* Group the data based on a specific range
* Helps understand the correlation between two measures.
* 
* Formula
* [Mesuare] by [Mesuare] 
*/

/*
 * Segment products into cost ranges and count how many products fall into each segment
 * */
WITH product_segments AS (
/*
 * Generate a category from a measure
 * We segment the cost into three categories
 * We are taking one of our measures and comberting into a dimension using case when statement
 * */
	SELECT
		product_key,
		product_name,
		cost,
		CASE 
			WHEN cost < 100 THEN 'Below 100'
			WHEN cost BETWEEN 100 AND 500 THEN '100-500'
			WHEN cost BETWEEN 500 AND 1000 THEN '500-1000'
			ELSE 'Above 1000'
		END cost_range
	FROM gold.dim_products
)
SELECT
	cost_range,
	COUNT(product_key) AS total_products
FROM product_segments
GROUP BY cost_range
ORDER BY total_products DESC; 


/*
 * Group customer into three segments based on their spending behavior:
 * 	- VIP: Customers with at least 12 months of history and spending more than 5,000.
 * 	- Regular: Customers with at least 12 months of history but spending 5,000 or less.
 * 	- New: Customers with a lifespan less than 12 months.
 * And find the total number of customers by each group.
 * */

WITH customer_spending AS (
	SELECT
		f.customer_key,
		SUM(f.sales) AS total_spending,
		MIN(f.order_date) AS first_order,
		MAX(f.order_date) AS last_order,
		(EXTRACT(YEAR FROM AGE(MAX(order_date), MIN(order_date))) * 12) + EXTRACT(MONTH FROM AGE(MAX(order_date), MIN(order_date))) AS lifespan
	FROM gold.fact_sales f
	LEFT JOIN gold.dim_customers c
	ON f.customer_key = c.customer_key
	GROUP BY f.customer_key
)
SELECT
	t.customer_segment,
	COUNT(t.customer_key)
FROM (
	SELECT
		customer_key,
		CASE 
			WHEN lifespan >= 12 AND total_spending > 5000 THEN 'VIP'
			WHEN lifespan >= 12 AND total_spending <= 5000 THEN 'Regular'
			ELSE 'New'
		END customer_segment
	FROM customer_spending
) AS t
GROUP BY t.customer_segment;