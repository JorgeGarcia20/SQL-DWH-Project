/*
=================================================
Store procedure: Load Bronze Layer (source -> bronze)
=================================================
Script purpose:
    This stored procedure loads data into the bronze schema from external CSV files.
    It performs the following steps:
    1. Truncates the target tables in the bronze schema to ensure a clean load.
    2. Loads data from CSV files into the respective tables using the COPY command.
    3. Logs the time taken for each table load and the total time taken for the entire loading process.
    4. Handles any exceptions that may occur during the loading process and logs the error message and code.
Tables involved:
    - bronze.crm_cust_info
    - bronze.crm_prd_info
    - bronze.crm_sales_details
    - bronze.erp_cust_az12
    - bronze.erp_loc_a101
    - bronze.erp_px_cat_g1v2

Usage:
    CALL bronze.load_bronze();
*/

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
    v_time_start TIMESTAMP;
    v_time_end TIMESTAMP;
    v_time_load_start TIMESTAMP;
    v_time_load_end TIMESTAMP;
BEGIN
    v_time_start := clock_timestamp();
    BEGIN
        RAISE NOTICE '========================================';
        RAISE NOTICE 'Loading data into bronze layer';
        RAISE NOTICE '========================================';

        RAISE NOTICE '-----------------------------------------';
        RAISE NOTICE 'Loading CRM tables';
        RAISE NOTICE '-----------------------------------------';
        
        v_time_load_start := clock_timestamp();
        RAISE NOTICE '>> Truncating table: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;

        RAISE NOTICE '>> Loading data into table: bronze.crm_cust_info';
        COPY bronze.crm_cust_info(cst_id, cst_key, cst_first_name, cst_last_name, cst_maritial_status, cst_gnder, cst_create_date)
        FROM '/datasets/source_crm/cust_info.csv'
        DELIMITER ','
        CSV HEADER;
        v_time_load_end := clock_timestamp();
        RAISE NOTICE 'Time taken to load bronze.crm_cust_info: % milliseconds', EXTRACT(MILLISECONDS FROM (v_time_load_end - v_time_load_start));

        -- v_time_load_start := clock_timestamp();
        -- RAISE NOTICE '>> Truncating table: bronze.crm_prd_info';
        -- TRUNCATE TABLE bronze.crm_prd_info;

        -- RAISE NOTICE '>> Loading data into table: bronze.crm_prd_info';
        -- COPY bronze.crm_prd_info(prd_id, prd_key, prd_nm, prd_cost, prd_start_dt, prd_end_dt)
        -- FROM '/datasets/source_crm/prd_info.csv'
        -- DELIMITER ','
        -- CSV HEADER;
        -- v_time_load_end := clock_timestamp();
        -- RAISE NOTICE 'Time taken to load bronze.crm_prd_info: % milliseconds', EXTRACT(MILLISECONDS FROM (v_time_load_end - v_time_load_start));

        v_time_load_start := clock_timestamp();
        RAISE NOTICE '>> Truncating table: bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;

        RAISE NOTICE '>> Loading data into table: bronze.crm_sales_details';
        COPY bronze.crm_sales_details(sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price)
        FROM '/datasets/source_crm/sales_details.csv'
        DELIMITER ','
        CSV HEADER;
        v_time_load_end := clock_timestamp();
        RAISE NOTICE 'Time taken to load bronze.crm_sales_details: % milliseconds', EXTRACT(MILLISECONDS FROM (v_time_load_end - v_time_load_start));

        RAISE NOTICE '-----------------------------------------';
        RAISE NOTICE 'Loading ERP tables';
        RAISE NOTICE '-----------------------------------------';

        v_time_load_start := clock_timestamp();
        RAISE NOTICE '>> Truncating table: bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;

        RAISE NOTICE '>> Loading data into table: bronze.erp_cust_az12';
        COPY bronze.erp_cust_az12(cid, bdate, gen)
        FROM '/datasets/source_erp/cust_az12.csv'
        DELIMITER ','
        CSV HEADER;
        v_time_load_end := clock_timestamp();
        RAISE NOTICE 'Time taken to load bronze.erp_cust_az12: % milliseconds', EXTRACT(MILLISECONDS FROM (v_time_load_end - v_time_load_start));

        v_time_load_start := clock_timestamp();
        RAISE NOTICE '>> Truncating table: bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;

        RAISE NOTICE '>> Loading data into table: bronze.erp_loc_a101';
        COPY bronze.erp_loc_a101(cid, cntry)
        FROM '/datasets/source_erp/loc_a101.csv' 
        DELIMITER ','
        CSV HEADER;
        v_time_load_end := clock_timestamp();
        RAISE NOTICE 'Time taken to load bronze.erp_loc_a101: % milliseconds', EXTRACT(MILLISECONDS FROM (v_time_load_end - v_time_load_start));

        v_time_load_start := clock_timestamp();
        RAISE NOTICE '>> Truncating table: bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        RAISE NOTICE '>> Loading data into table: bronze.erp_px_cat_g1v2';
        COPY bronze.erp_px_cat_g1v2(id, cat, subcat, maintence)
        FROM '/datasets/source_erp/px_cat_g1v2.csv'
        DELIMITER ','
        CSV HEADER;
        v_time_load_end := clock_timestamp();
        RAISE NOTICE 'Time taken to load bronze.erp_px_cat_g1v2: % milliseconds', EXTRACT(MILLISECONDS FROM (v_time_load_end - v_time_load_start));
    
        v_time_end := clock_timestamp();
        RAISE NOTICE '========================================';
        RAISE NOTICE 'Data loading completed successfully';
        RAISE NOTICE 'Total time taken to load data into bronze layer: % seconds', EXTRACT(MILLISECONDS FROM (v_time_end - v_time_start)) / 1000.0;
        RAISE NOTICE '========================================';
    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE '========================================';
            RAISE NOTICE 'Error: %, Código: %', SQLERRM, SQLSTATE;
            RAISE NOTICE '========================================';
    END;
END;
$$;