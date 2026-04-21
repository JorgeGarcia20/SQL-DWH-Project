-- Current[Measure] - Target[Measure]
-- Current Sales - Average Sales
-- Current year sales - Previus Year Sales
-- Current Sales - Lowes Sales


-- Analyze the yearly performance of products by comparing their sales
-- to both the average sales performance of the product and the previous year's sales

WITH yearly_product_sales AS (
	SELECT
		EXTRACT(YEAR FROM f.order_date) AS order_year,
		p.product_name,
		SUM(f.sales) AS current_sales
	FROM gold.fact_sales f
	LEFT JOIN gold.dim_products p
	ON f.product_key = p.product_key
	WHERE f.order_date IS NOT NULL
	GROUP BY order_year, p.product_name
)
SELECT
	order_year,
	product_name,
	current_sales,
	AVG(current_sales) OVER (PARTITION BY product_name) AS avg_sales,
	current_sales - AVG(current_sales) OVER (PARTITION BY product_name) AS diff_avg,
	CASE WHEN current_sales - AVG(current_sales) OVER (PARTITION BY product_name) > 0 THEN 'Above Avg'
		WHEN current_sales - AVG(current_sales) OVER (PARTITION BY product_name) < 0 THEN 'Below Avg'
		ELSE 'Avg'
	END avg_change,
	
	-- Year over year analysis
	COALESCE(LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year), 0) AS py_sales,
	current_sales - COALESCE(LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year), 0) AS diff_py,
	CASE WHEN current_sales - COALESCE(LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year), 0) > 0 THEN 'Increase'
		WHEN current_sales - COALESCE(LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year), 0) < 0 THEN 'Decrease'
		ELSE 'No Change'
	END py_change
FROM yearly_product_sales;