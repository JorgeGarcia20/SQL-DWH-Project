/*
=================================================
Store procedure: Load Silver Layer (bronze -> silver)
=================================================
Script purpose:
    This stored procedure loads data into the silver schema from the bronze schema.
    It performs the following steps:
    1. Truncates the target tables in the silver schema to ensure a clean load.
    2. Transforms and loads data from bronze tables into the respective silver tables using INSERT INTO ... SELECT statements.
    3. Logs the time taken for each table load and the total time taken for the entire loading process.
    4. Handles any exceptions that may occur during the loading process and logs the error message and code.

Cleansing and transformation rules applied:
    - For silver.crm_cust_info:
        - Trim whitespace from first and last names.
        - Map marital status codes ('S', 'M') to 'Single' and 'Married', respectively; set unknown values to 'n/a'.
        - Map gender codes ('F', 'M') to 'Female' and 'Male', respectively; set unknown values to 'n/a'.
        - Keep the most recent record for each customer based on cst_create_date.
    - For silver.crm_prd_info:
        - Extract cat_id from the first 5 characters of prd_key, replacing '-' with '_'.
        - Extract prd_key from the substring of prd_key starting from the 7th character.
        - Map product line codes ('M', 'R', 'S', 'T') to 'Mauntain', 'Road', 'Other Sales', and 'Touring', respectively; set unknown values to 'n/a'.
        - Convert prd_start_dt to date and calculate prd_end_dt as the day before the next prd_start_dt for the same prd_key.
    - For silver.crm_sales_details:
        - Convert sls_order_dt, sls_ship_dt, and sls_due_dt from integer to date format; set invalid dates (0 or out of reasonable range) to NULL.
        - Calculate sls_sales as sls_quantity * abs(sls_price) if sls_sales is null, zero, negative, or not equal to sls_quantity * abs(sls_price).
        - Calculate sls_price as sls_sales / sls_quantity if sls_price is null or zero.
    - For silver.erp_cust_az12:
        - Remove 'NAS' prefix from cid if present.
        - Set future birthdates to NULL.
        - Map gender values ('F', 'M', 'FEMALE', 'MALE') to 'Female' and 'Male', respectively; set unknown values to 'n/a'.
    - For silver.erp_loc_a101:
        - Remove dashes from cid.
        - Map country codes ('DE', 'US', 'USA') to 'Germany' and 'United States', respectively; set empty or null values to NULL; keep other values as is.
    - For silver.erp_px_cat_g1v2:
        - No transformations needed; data is loaded as is from bronze.

Tables involved:
    - silver.crm_cust_info
    - silver.crm_prd_info
    - silver.crm_sales_details
    - silver.erp_cust_az12
    - silver.erp_loc_a101
    - silver.erp_px_cat_g1v2
Usage:
    CALL silver.load_silver();
*/

CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
DECLARE
    v_time_start TIMESTAMP;
    v_time_END TIMESTAMP;
    v_time_load_start TIMESTAMP;
    v_time_load_end TIMESTAMP;
BEGIN
    v_time_start := clock_timestamp();
    BEGIN
        RAISE NOTICE '========================================';
        RAISE NOTICE 'Loading data into silver layer';
        RAISE NOTICE '========================================';

        RAISE NOTICE '-----------------------------------------';
        RAISE NOTICE 'Loading CRM tables';
        RAISE NOTICE '-----------------------------------------';

        v_time_load_start := clock_timestamp();
        RAISE NOTICE '>> Truncating table: silver.crm_cust_info';
        TRUNCATE TABLE silver.crm_cust_info;

        RAISE NOTICE '>> Loading data into table: silver.crm_cust_info';
        INSERT INTO silver.crm_cust_info (
            cst_id,
            cst_key,
            cst_first_name, 
            cst_last_name, 
            cst_maritial_status,
            cst_gnder,
            cst_create_date
        )
        SELECT 
            cst_id,
            cst_key,
            TRIM(cst_first_name) AS cst_first_name, 
            TRIM(cst_last_name) AS cst_last_name, 

            CASE WHEN UPPER(TRIM(cst_maritial_status)) = 'S' THEN 'Single'
                WHEN UPPER(TRIM(cst_maritial_status)) = 'M' THEN 'Married'
                ELSE 'n/a' 
            END cst_maritial_status,

            CASE WHEN UPPER(TRIM(cst_gnder)) = 'F' THEN 'Female'
                WHEN UPPER(TRIM(cst_gnder)) = 'M' THEN 'Male'
                ELSE 'n/a' 
            END cst_gnder,
            cst_create_date
        FROM (
            SELECT *,
            row_number() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS rn
            FROM bronze.crm_cust_info
        ) AS t
        WHERE t.rn = 1;
        v_time_load_end := clock_timestamp();
        RAISE NOTICE 'Time taken to load silver.crm_cust_info: % milliseconds', EXTRACT(MILLISECONDS FROM (v_time_load_end - v_time_load_start));

        v_time_load_start := clock_timestamp();
        RAISE NOTICE '>> Truncating table: silver.crm_prd_info';
        TRUNCATE TABLE silver.crm_prd_info;

        RAISE NOTICE '>> Loading data into table: silver.crm_prd_info';
        INSERT INTO silver.crm_prd_info (
            prd_id,
            cat_id,
            prd_key,
            prd_nm, 
            prd_cost,
            prd_line, 
            prd_start_dt, 
            prd_end_dt
        )
        SELECT 
            prd_id,
            REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
            SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key,
            prd_nm, 
            COALESCE(prd_cost, 0) AS prd_cost,
            CASE UPPER(TRIM(prd_line))
                WHEN 'M' THEN 'Mauntain'
                WHEN 'R' THEN 'Road'
                WHEN 'S' THEN 'Other Sales'
                WHEN 'T' THEN 'Touring'
                ELSE 'n/a'
            END AS prd_line, 
            CAST(prd_start_dt AS date) AS prd_start_dt, 
            CAST(lead(prd_start_dt) over(partition by prd_key order by prd_start_dt) AS date) - 1 AS prd_end_dt
        FROM bronze.crm_prd_info;
        v_time_load_end := clock_timestamp();
        RAISE NOTICE 'Time taken to load silver.crm_prd_info: % milliseconds', EXTRACT(MILLISECONDS FROM (v_time_load_end - v_time_load_start));

        v_time_load_start := clock_timestamp();
        RAISE NOTICE '>> Truncating table: silver.crm_sales_details';
        TRUNCATE TABLE silver.crm_sales_details;

        RAISE NOTICE '>> Loading data into table: silver.crm_sales_details';    
        INSERT INTO silver.crm_sales_details (
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            sls_order_dt,
            sls_ship_dt,
            sls_due_dt,
            sls_sales,
            sls_quantity,
            sls_price
        )
        SELECT 
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            CASE 
                WHEN sls_order_dt = 0 or sls_order_dt > 20500101 or sls_order_dt < 19000101 THEN null
                ELSE CAST(CAST(sls_order_dt AS varchar) AS date)
            END sls_order_dt,
            CASE 
                WHEN sls_ship_dt = 0 or sls_ship_dt > 20500101 or sls_ship_dt < 19000101 THEN null
                ELSE CAST(CAST(sls_ship_dt AS varchar) AS date)
            END sls_ship_dt,
            CASE 
                WHEN sls_due_dt = 0 or sls_due_dt > 20500101 or sls_due_dt < 19000101 THEN null
                ELSE CAST(CAST(sls_due_dt AS varchar) AS date)
            END sls_due_dt,
            CASE 
                WHEN sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * abs(sls_price) THEN sls_quantity * abs(sls_price) 
                ELSE sls_sales 
            END AS sls_sales,  -- sales = quantity * price, null, zero or negative sales are not valid
            sls_quantity,
            CASE 
                WHEN sls_price is null or sls_price <= 0 THEN sls_sales / COALESCE(sls_quantity, 0)
                ELSE sls_price
            END AS sls_price
        FROM bronze.crm_sales_details;
        v_time_load_end := clock_timestamp();
        RAISE NOTICE 'Time taken to load silver.crm_sales_details: % milliseconds', EXTRACT(MILLISECONDS FROM (v_time_load_end - v_time_load_start));

        RAISE NOTICE '-----------------------------------------';
        RAISE NOTICE 'Loading ERP tables';
        RAISE NOTICE '-----------------------------------------';

        v_time_load_start := clock_timestamp();
        RAISE NOTICE '>> Truncating table: silver.erp_cust_az12';
        TRUNCATE TABLE silver.erp_cust_az12;

        RAISE NOTICE '>> Loading data into table: silver.erp_cust_az12';
        INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
        SELECT 
            CASE 
                WHEN cid like 'NAS%' THEN SUBSTRING(cid, 4) 
                ELSE cid 
            END AS cid, -- Remove 'NAS' prefix if present.
            CASE 
                WHEN bdate > current_timestamp THEN null 
                ELSE bdate 
            END AS bdate, -- Set future birthdates to NULL
            CASE 
                WHEN UPPER(TRIM(gen)) in ('F', 'FEMALE') THEN 'Female' 
                WHEN UPPER(TRIM(gen)) in ('M', 'MALE') THEN 'Male' 
                ELSE 'n/a' 
            END AS gen -- Normalize gENDer values and handle unknown CASEs
        FROM bronze.erp_cust_az12;
        v_time_load_end := clock_timestamp();
        RAISE NOTICE 'Time taken to load silver.erp_cust_az12: % milliseconds', EXTRACT(MILLISECONDS FROM (v_time_load_end - v_time_load_start));

        v_time_load_start := clock_timestamp();
        RAISE NOTICE '>> Truncating table: silver.erp_loc_a101';
        TRUNCATE TABLE silver.erp_loc_a101;

        RAISE NOTICE '>> Loading data into table: silver.erp_loc_a101';
        INSERT INTO silver.erp_loc_a101 (cid, cntry)
        SELECT
            REPLACE(cid, '-', '') AS cid,
            CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
                WHEN TRIM(cntry) in ('US', 'USA') THEN 'United States'
                WHEN TRIM(cntry) = '' or cntry is null THEN null
                ELSE cntry
            END AS cntry
        FROM bronze.erp_loc_a101;
        v_time_load_end := clock_timestamp();
        RAISE NOTICE 'Time taken to load silver.erp_loc_a101: % milliseconds', EXTRACT(MILLISECONDS FROM (v_time_load_end - v_time_load_start));

        v_time_load_start := clock_timestamp();
        RAISE NOTICE '>> Truncating table: silver.erp_px_cat_g1v2';
        TRUNCATE TABLE silver.erp_px_cat_g1v2;

        RAISE NOTICE '>> Loading data into table: silver.erp_px_cat_g1v2';
        INSERT INTO silver.erp_px_cat_g1v2(id, cat, subcat, maintence)
        SELECT
            id,
            cat,
            subcat,
            maintence 
        FROM bronze.erp_px_cat_g1v2;
        v_time_load_end := clock_timestamp();
        RAISE NOTICE 'Time taken to load silver.erp_px_cat_g1v2: % milliseconds', EXTRACT(MILLISECONDS FROM (v_time_load_end - v_time_load_start));

        v_time_end := clock_timestamp();
        RAISE NOTICE '========================================';
        RAISE NOTICE 'Data loading completed successfully';
        RAISE NOTICE 'Total time taken to load silver layer: % milliseconds', EXTRACT(MILLISECONDS FROM (v_time_end - v_time_start));
        RAISE NOTICE '========================================';
    
    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE '=========================================';
            RAISE NOTICE 'Data loading failed';
            RAISE NOTICE 'Error: %, Code: %', SQLERRM, SQLSTATE;
            RAISE NOTICE '=========================================';
    END;
END;
$$;