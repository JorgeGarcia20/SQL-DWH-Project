-- Comulative analysis
-- Aggregate data progressively over time.
-- Helps to understand whether the business is growing or declining.
-- [columative mesure] by [date dimension]

-- Calculate the total sales per month and the running total of sales over times.
SELECT
t.order_month,
t.total_sales,
SUM(t.total_sales) OVER(ORDER BY t.order_month ASC) as running_total_sales
FROM (
	SELECT
		DATE(DATE_TRUNC('month', order_date)) AS order_month,
		SUM(sales) AS total_sales
	FROM gold.fact_sales
	WHERE order_date IS NOT NULL
	GROUP BY order_month
) AS t;

-- Calculate the total sales per month and the running total of sales over years.
SELECT
t.order_month,
t.total_sales,
SUM(t.total_sales) OVER(PARTITION BY DATE_TRUNC('year', t.order_month) ORDER BY t.order_month ASC) as running_total_sales
FROM (
	SELECT
		DATE(DATE_TRUNC('month', order_date)) AS order_month,
		SUM(sales) AS total_sales
	FROM gold.fact_sales
	WHERE order_date IS NOT NULL
	GROUP BY order_month
) AS t;

-- Calculate the total sales per year and the running total of sales over times.
SELECT
t.order_year,
t.total_sales,
SUM(t.total_sales) OVER(ORDER BY t.order_year ASC) as running_total_sales
FROM (
	SELECT
		DATE(DATE_TRUNC('year', order_date)) AS order_year,
		SUM(sales) AS total_sales
	FROM gold.fact_sales
	WHERE order_date IS NOT NULL
	GROUP BY order_year
) AS t;
