/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Author: Sneh Sparsh
Project Name: CRM ERP Data Warehouse Project
Created Date: 15-02-2025
===============================================================================
Script Purpose:
    This stored procedure loads transformed data into the 'silver' schema from 'bronze' schema
    It performs the following actions:
    - Truncates the silver tables before loading data.
    - Extracts, Clean, Transform the bronze layer tables data before loading to silver layer tables.
    - Logs the row counts before truncating, after truncating, and after loading.
    - Inserts audit logs into the etl_audit_log table for tracking ETL operations.

Parameters:
    None. 
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC silver.load_silver;

Dependencies:
    - bronze schema with source tables.
    - silver schema with target tables (crm_cust_info, crm_prd_info, crm_sales_details,
      erp_loc_a101, erp_cust_az12, erp_px_cat_g1v2).
    - etl_audit_log table with columns:
        (procedure_name NVARCHAR(255), table_name NVARCHAR(255),
         start_time DATETIME, end_time DATETIME,
         rows_before INT, rows_deleted INT, rows_after INT,
         load_duration INT, status NVARCHAR(50), error_message NVARCHAR(MAX))

===============================================================================
Script Update History:
| Name        | Update Date | Comments                                                                 |
|-------------|-------------|--------------------------------------------------------------------------|
| Sneh Sparsh | 22-04-2025  | Added variable to capture rows before load, deleted rows, and loaded rows to print in logs |
| Sneh Sparsh | 23-04-2025  | Added logic to insert all the logs in ETL Audit Log table for each source |
| Sneh Sparsh | 28-04-2025  | Enhanced PRINT messages for clearer output. Added SET NOCOUNT ON/OFF.  |
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    -- Suppress the automatic row count messages for cleaner output
    SET NOCOUNT ON;

    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
    DECLARE @row_count_before INT, @row_count_after INT, @deleted_count INT;
    DECLARE @procedure_name NVARCHAR(255) = 'silver.load_silver';
    DECLARE @table_name NVARCHAR(255);
    DECLARE @status NVARCHAR(50);
    DECLARE @error_message NVARCHAR(MAX);

    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '====================================================================';
        PRINT '[INFO] ETL Process Started: Loading Silver Layer';
        PRINT '[INFO] Start Time (Batch): ' + CONVERT(NVARCHAR, @batch_start_time, 120); -- ISO 8601 format
        PRINT '====================================================================';
        PRINT ''; -- Add a blank line for readability

        PRINT '--------------------------------------------------------------------';
        PRINT '[INFO] Starting CRM Tables Load';
        PRINT '--------------------------------------------------------------------';
        PRINT '';

        -- CRM Customer Info
        SET @table_name = 'CRM_ERP_DWH.silver.crm_cust_info';
        SET @start_time = GETDATE();
        PRINT '[INFO] Processing Table: ' + @table_name;
        PRINT '[INFO] Start Time: ' + CONVERT(NVARCHAR, @start_time, 120);

        SELECT @row_count_before = COUNT(*) FROM CRM_ERP_DWH.silver.crm_cust_info;
        PRINT '[INFO] Rows Before Truncate: ' + CAST(@row_count_before AS NVARCHAR);

        PRINT '[INFO] Truncating Table: ' + @table_name;
        TRUNCATE TABLE CRM_ERP_DWH.silver.crm_cust_info;
        SET @deleted_count = @row_count_before; -- For truncate, deleted count is same as count before
        PRINT '[SUCCESS] Table Truncated. Deleted Count: ' + CAST(@deleted_count AS NVARCHAR);

        PRINT '[INFO] Inserting Data Into: ' + @table_name;
        INSERT INTO CRM_ERP_DWH.silver.crm_cust_info (
                    cst_id,
                    cst_key,
                    cst_firstname,
                    cst_lastname,
                    cst_marital_status,
                    cst_gndr,
                    cst_create_date
                    )
        SELECT
            cst_id,
            cst_key,
            TRIM(cst_firstname) AS cst_firstname,
            TRIM(cst_lastname) AS cst_lastname ,
            CASE
                WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
                WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
                ELSE 'n/a'
            END AS cst_marital_status,
            CASE
                WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
                WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
                ELSE 'n/a'
            END AS cst_gndr,
            cst_create_date
        FROM (
            SELECT * , ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
            FROM CRM_ERP_DWH.bronze.crm_cust_info
            WHERE cst_id IS NOT NULL
            ) t
        WHERE flag_last = 1;

        SELECT @row_count_after = COUNT(*) FROM CRM_ERP_DWH.silver.crm_cust_info;
        PRINT '[SUCCESS] Data Inserted. Rows Loaded: ' + CAST(@row_count_after AS NVARCHAR);
        SET @end_time = GETDATE();
        PRINT '[INFO] Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        PRINT '[INFO] Inserting Audit Log for ' + @table_name;
        INSERT INTO etl_audit_log (procedure_name, table_name, start_time, end_time, rows_before, rows_deleted, rows_after, load_duration, status)
        VALUES (@procedure_name, @table_name, @start_time, @end_time, @row_count_before, @deleted_count, @row_count_after, DATEDIFF(SECOND, @start_time, @end_time), 'Success');
        PRINT '[SUCCESS] Audit Log Inserted for ' + @table_name;
        PRINT '--------------------------------------------------------------------';
        PRINT '[INFO] Finished Processing Table: ' + @table_name;
        PRINT '[INFO] End Time: ' + CONVERT(NVARCHAR, @end_time, 120);
        PRINT '--------------------------------------------------------------------';
        PRINT ''; -- Add a blank line between tables


        -- CRM Product Info
        SET @table_name = 'CRM_ERP_DWH.silver.crm_prd_info';
        SET @start_time = GETDATE();
        PRINT '[INFO] Processing Table: ' + @table_name;
        PRINT '[INFO] Start Time: ' + CONVERT(NVARCHAR, @start_time, 120);

        SELECT @row_count_before = COUNT(*) FROM CRM_ERP_DWH.silver.crm_prd_info;
        PRINT '[INFO] Rows Before Truncate: ' + CAST(@row_count_before AS NVARCHAR);

        PRINT '[INFO] Truncating Table: ' + @table_name;
        TRUNCATE TABLE CRM_ERP_DWH.silver.crm_prd_info;
        SELECT @deleted_count = @row_count_before;
        PRINT '[SUCCESS] Table Truncated. Deleted Count: ' + CAST(@deleted_count AS NVARCHAR);

        PRINT '[INFO] Inserting Data Into: ' + @table_name;
        INSERT INTO CRM_ERP_DWH.silver.crm_prd_info (
             prd_id
            ,cat_id
            ,prd_key
            ,prd_nm
            ,prd_cost
            ,prd_line
            ,prd_start_dt
            ,prd_end_dt
        )
        SELECT prd_id
             ,REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id -- Extract category ID
             ,SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key -- Extract product Key
             ,prd_nm
             ,ISNULL(prd_cost, 0) AS prd_cost
             ,CASE UPPER(TRIM(prd_line))
                    WHEN 'M' THEN 'Mountain'
                    WHEN 'R' THEN 'Road'
                    WHEN 'S' THEN 'Other Sales'
                    WHEN 'T' THEN 'Touring'
                    ELSE 'n/a'
            END AS prd_line -- Map product line codes to descriptive values
             ,CAST (prd_start_dt AS DATE) AS prd_start_dt
             ,CAST (LEAD(prd_start_dt, 1) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE) AS prd_end_dt
          FROM CRM_ERP_DWH.bronze.crm_prd_info;

        SELECT @row_count_after = COUNT(*) FROM CRM_ERP_DWH.silver.crm_prd_info;
        PRINT '[SUCCESS] Data Inserted. Rows Loaded: ' + CAST(@row_count_after AS NVARCHAR);
        SET @end_time = GETDATE();
        PRINT '[INFO] Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        PRINT '[INFO] Inserting Audit Log for ' + @table_name;
        INSERT INTO etl_audit_log (procedure_name, table_name, start_time, end_time, rows_before, rows_deleted, rows_after, load_duration, status)
        VALUES (@procedure_name, @table_name, @start_time, @end_time, @row_count_before, @deleted_count, @row_count_after, DATEDIFF(SECOND, @start_time, @end_time), 'Success');
        PRINT '[SUCCESS] Audit Log Inserted for ' + @table_name;
        PRINT '--------------------------------------------------------------------';
        PRINT '[INFO] Finished Processing Table: ' + @table_name;
        PRINT '[INFO] End Time: ' + CONVERT(NVARCHAR, @end_time, 120);
        PRINT '--------------------------------------------------------------------';
        PRINT ''; -- Add a blank line between tables


        -- CRM Sales Details
        SET @table_name = 'CRM_ERP_DWH.silver.crm_sales_details';
        SET @start_time = GETDATE();
        PRINT '[INFO] Processing Table: ' + @table_name;
        PRINT '[INFO] Start Time: ' + CONVERT(NVARCHAR, @start_time, 120);

        SELECT @row_count_before = COUNT(*) FROM CRM_ERP_DWH.silver.crm_sales_details;
        PRINT '[INFO] Rows Before Truncate: ' + CAST(@row_count_before AS NVARCHAR);

        PRINT '[INFO] Truncating Table: ' + @table_name;
        TRUNCATE TABLE CRM_ERP_DWH.silver.crm_sales_details;
        SELECT @deleted_count = @row_count_before;
        PRINT '[SUCCESS] Table Truncated. Deleted Count: ' + CAST(@deleted_count AS NVARCHAR);

        PRINT '[INFO] Inserting Data Into: ' + @table_name;
        INSERT INTO CRM_ERP_DWH.silver.crm_sales_details (
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
              -- Transformation: Convert date strings (yyyymmdd int) to DATE, handle invalid formats
              CASE WHEN sls_order_dt IS NULL OR sls_order_dt = 0 OR LEN(CAST(sls_order_dt AS VARCHAR)) <> 8 THEN NULL
                   ELSE TRY_CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) -- Use TRY_CAST for robust conversion
              END AS sls_order_dt,
              CASE WHEN sls_ship_dt IS NULL OR sls_ship_dt = 0 OR LEN(CAST(sls_ship_dt AS VARCHAR)) <> 8 THEN NULL
                   ELSE TRY_CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
              END AS sls_ship_dt,
              CASE WHEN sls_due_dt IS NULL OR sls_due_dt = 0 OR LEN(CAST(sls_due_dt AS VARCHAR)) <> 8 THEN NULL
                   ELSE TRY_CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
              END AS sls_due_dt,
            CASE
                WHEN sls_sales IS NULL OR sls_sales <= 0 OR ABS(sls_sales) <> ABS(sls_quantity * sls_price)
                THEN ABS(sls_quantity * sls_price)
                ELSE ABS(sls_sales)
            END AS sls_sales,
            sls_quantity,
            CASE
                WHEN sls_price IS NULL OR sls_price = 0 OR ABS(sls_price) <> ABS(sls_sales / NULLIF(sls_quantity, 0))
                THEN ABS(sls_sales / NULLIF(sls_quantity, 0))
                ELSE ABS(sls_price)
            END AS sls_price
          FROM CRM_ERP_DWH.bronze.crm_sales_details;

        SELECT @row_count_after = COUNT(*) FROM CRM_ERP_DWH.silver.crm_sales_details; -- *** Corrected: Should count from silver table after load ***
        PRINT '[SUCCESS] Data Inserted. Rows Loaded: ' + CAST(@row_count_after AS NVARCHAR);
        SET @end_time = GETDATE();
        PRINT '[INFO] Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        PRINT '[INFO] Inserting Audit Log for ' + @table_name;
        INSERT INTO etl_audit_log (procedure_name, table_name, start_time, end_time, rows_before, rows_deleted, rows_after, load_duration, status)
        VALUES (@procedure_name, @table_name, @start_time, @end_time, @row_count_before, @deleted_count, @row_count_after, DATEDIFF(SECOND, @start_time, @end_time), 'Success');
        PRINT '[SUCCESS] Audit Log Inserted for ' + @table_name;
        PRINT '--------------------------------------------------------------------';
        PRINT '[INFO] Finished Processing Table: ' + @table_name;
        PRINT '[INFO] End Time: ' + CONVERT(NVARCHAR, @end_time, 120);
        PRINT '--------------------------------------------------------------------';
        PRINT ''; -- Add a blank line between tables


        PRINT '--------------------------------------------------------------------';
        PRINT '[INFO] Starting ERP Tables Load';
        PRINT '--------------------------------------------------------------------';
        PRINT '';

        -- ERP Location A101 (ERP Location Info)
        SET @table_name = 'CRM_ERP_DWH.silver.erp_loc_a101';
        SET @start_time = GETDATE();
        PRINT '[INFO] Processing Table: ' + @table_name;
        PRINT '[INFO] Start Time: ' + CONVERT(NVARCHAR, @start_time, 120);

        SELECT @row_count_before = COUNT(*) FROM CRM_ERP_DWH.silver.erp_loc_a101;
        PRINT '[INFO] Rows Before Truncate: ' + CAST(@row_count_before AS NVARCHAR);

        PRINT '[INFO] Truncating Table: ' + @table_name;
        TRUNCATE TABLE CRM_ERP_DWH.silver.erp_loc_a101;
        SELECT @deleted_count = @row_count_before;
        PRINT '[SUCCESS] Table Truncated. Deleted Count: ' + CAST(@deleted_count AS NVARCHAR);

        PRINT '[INFO] Inserting Data Into: ' + @table_name;
        INSERT INTO CRM_ERP_DWH.silver.erp_loc_a101 (
            cid,
            cntry
        )
        SELECT
            -- Removing invalid value
            REPLACE(cid, '-', '') as cid,
            -- Normalize and Handle missing or blank country names
            CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
                 WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
                 WHEN TRIM(cntry) IS NULL OR TRIM(cntry) = '' THEN 'n/a' -- Handle NULL or empty string
                 ELSE TRIM(cntry) -- Trim other country names
            END AS cntry
        FROM CRM_ERP_DWH.bronze.erp_loc_a101;

        SELECT @row_count_after = COUNT(*) FROM CRM_ERP_DWH.silver.erp_loc_a101; -- *** Corrected: Should count from silver table after load ***
        PRINT '[SUCCESS] Data Inserted. Rows Loaded: ' + CAST(@row_count_after AS NVARCHAR);
        SET @end_time = GETDATE();
        PRINT '[INFO] Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        PRINT '[INFO] Inserting Audit Log for ' + @table_name;
        INSERT INTO etl_audit_log (procedure_name, table_name, start_time, end_time, rows_before, rows_deleted, rows_after, load_duration, status)
        VALUES (@procedure_name, @table_name, @start_time, @end_time, @row_count_before, @deleted_count, @row_count_after, DATEDIFF(SECOND, @start_time, @end_time), 'Success');
        PRINT '[SUCCESS] Audit Log Inserted for ' + @table_name;
        PRINT '--------------------------------------------------------------------';
        PRINT '[INFO] Finished Processing Table: ' + @table_name;
        PRINT '[INFO] End Time: ' + CONVERT(NVARCHAR, @end_time, 120);
        PRINT '--------------------------------------------------------------------';
        PRINT ''; -- Add a blank line between tables


        -- ERP Customer AZ12 (ERP Customer Info)
        SET @table_name = 'CRM_ERP_DWH.silver.erp_cust_az12';
        SET @start_time = GETDATE();
        PRINT '[INFO] Processing Table: ' + @table_name;
        PRINT '[INFO] Start Time: ' + CONVERT(NVARCHAR, @start_time, 120);

        SELECT @row_count_before = COUNT(*) FROM CRM_ERP_DWH.silver.erp_cust_az12;
        PRINT '[INFO] Rows Before Truncate: ' + CAST(@row_count_before AS NVARCHAR);

        PRINT '[INFO] Truncating Table: ' + @table_name;
        TRUNCATE TABLE CRM_ERP_DWH.silver.erp_cust_az12;
        SELECT @deleted_count = @row_count_before;
        PRINT '[SUCCESS] Table Truncated. Deleted Count: ' + CAST(@deleted_count AS NVARCHAR);

        PRINT '[INFO] Inserting Data Into: ' + @table_name;
        INSERT INTO CRM_ERP_DWH.silver.erp_cust_az12 (
            cid,
            bdate,
            gen
        )
        SELECT
            -- Transformation: Remove 'NAS' prefix from customer ID if present.
            CASE
                WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
                ELSE cid
            END AS cid,
            -- Transformation: Set birth dates in the future to NULL.
            CASE
                WHEN bdate > GETDATE() THEN NULL
                ELSE bdate
            END AS bdate,
            -- Transformation: Normalize gender values to 'Female', 'Male', or 'n/a'.
            CASE
                WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
                WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
                WHEN TRIM(gen) IS NULL OR TRIM(gen) = '' THEN 'n/a' -- Handle NULL or empty string
                ELSE 'n/a'
            END AS gen
        FROM
            CRM_ERP_DWH.bronze.erp_cust_az12;

        SELECT @row_count_after = COUNT(*) FROM CRM_ERP_DWH.silver.erp_cust_az12; -- *** Corrected: Should count from silver table after load ***
        PRINT '[SUCCESS] Data Inserted. Rows Loaded: ' + CAST(@row_count_after AS NVARCHAR);
        SET @end_time = GETDATE();
        PRINT '[INFO] Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        PRINT '[INFO] Inserting Audit Log for ' + @table_name;
        INSERT INTO etl_audit_log (procedure_name, table_name, start_time, end_time, rows_before, rows_deleted, rows_after, load_duration, status)
        VALUES (@procedure_name, @table_name, @start_time, @end_time, @row_count_before, @deleted_count, @row_count_after, DATEDIFF(SECOND, @start_time, @end_time), 'Success');
        PRINT '[SUCCESS] Audit Log Inserted for ' + @table_name;
        PRINT '--------------------------------------------------------------------';
        PRINT '[INFO] Finished Processing Table: ' + @table_name;
        PRINT '[INFO] End Time: ' + CONVERT(NVARCHAR, @end_time, 120);
        PRINT '--------------------------------------------------------------------';
        PRINT ''; -- Add a blank line between tables


        -- ERP PX Category G1V2 (ERP Product Category Info)
        SET @table_name = 'CRM_ERP_DWH.silver.erp_px_cat_g1v2';
        SET @start_time = GETDATE();
        PRINT '[INFO] Processing Table: ' + @table_name;
        PRINT '[INFO] Start Time: ' + CONVERT(NVARCHAR, @start_time, 120);

        SELECT @row_count_before = COUNT(*) FROM CRM_ERP_DWH.silver.erp_px_cat_g1v2;
        PRINT '[INFO] Rows Before Truncate: ' + CAST(@row_count_before AS NVARCHAR);

        PRINT '[INFO] Truncating Table: ' + @table_name;
        TRUNCATE TABLE CRM_ERP_DWH.silver.erp_px_cat_g1v2;
        SELECT @deleted_count = @row_count_before;
        PRINT '[SUCCESS] Table Truncated. Deleted Count: ' + CAST(@deleted_count AS NVARCHAR);

        PRINT '[INFO] Inserting Data Into: ' + @table_name;
        INSERT INTO CRM_ERP_DWH.silver.erp_px_cat_g1v2 (
            id,
            cat,
            subcat,
            maintenance
        )
        SELECT
            id,
            cat,
            subcat,
            maintenance
        FROM
            CRM_ERP_DWH.bronze.erp_px_cat_g1v2;

        SELECT @row_count_after = COUNT(*) FROM CRM_ERP_DWH.silver.erp_px_cat_g1v2; -- *** Corrected: Should count from silver table after load ***
        PRINT '[SUCCESS] Data Inserted. Rows Loaded: ' + CAST(@row_count_after AS NVARCHAR);
        SET @end_time = GETDATE();
        PRINT '[INFO] Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        PRINT '[INFO] Inserting Audit Log for ' + @table_name;
        INSERT INTO etl_audit_log (procedure_name, table_name, start_time, end_time, rows_before, rows_deleted, rows_after, load_duration, status)
        VALUES (@procedure_name, @table_name, @start_time, @end_time, @row_count_before, @deleted_count, @row_count_after, DATEDIFF(SECOND, @start_time, @end_time), 'Success');
        PRINT '[SUCCESS] Audit Log Inserted for ' + @table_name;
        PRINT '--------------------------------------------------------------------';
        PRINT '[INFO] Finished Processing Table: ' + @table_name;
        PRINT '[INFO] End Time: ' + CONVERT(NVARCHAR, @end_time, 120);
        PRINT '--------------------------------------------------------------------';
        PRINT ''; -- Add a blank line between tables


        SET @batch_end_time = GETDATE();
        PRINT '====================================================================';
        PRINT '[SUCCESS] ETL Process Completed: Loading Silver Layer Finished';
        PRINT '[INFO] End Time (Batch): ' + CONVERT(NVARCHAR, @batch_end_time, 120);
        PRINT '[INFO] Total Duration (Batch): ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
        PRINT '====================================================================';

    END TRY
    BEGIN CATCH
        SET @end_time = GETDATE(); -- Capture end time even on error
        SET @batch_end_time = GETDATE(); -- Capture batch end time on error
        SET @error_message = ERROR_MESSAGE();
        SET @status = 'Failed'; -- Set status to Failed

        PRINT ''; -- Add blank line for emphasis
        PRINT '====================================================================';
        PRINT '[ERROR] ETL Process Aborted: Error Loading Silver Layer';
        PRINT '[ERROR] Error Time: ' + CONVERT(NVARCHAR, GETDATE(), 120);
        PRINT '[ERROR] Error Message: ' + @error_message;
        PRINT '[ERROR] Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT '[ERROR] Error State: ' + CAST(ERROR_STATE() AS NVARCHAR);
        -- Note: @table_name here will be the table being processed when the error occurred
        PRINT '[ERROR] Failed Table (Approximate): ' + ISNULL(@table_name, 'N/A - Before first table');
        PRINT '====================================================================';

        -- Log the failure for the last table being processed (or the overall procedure if before any table)
        -- Re-using variables captured before the error (best effort for audit on failure)
         INSERT INTO etl_audit_log (procedure_name, table_name, start_time, end_time, rows_before, rows_deleted, rows_after, load_duration, status, error_message)
         VALUES (@procedure_name, ISNULL(@table_name, 'Overall Batch'), @start_time, @end_time, @row_count_before, @deleted_count, @row_count_after, DATEDIFF(SECOND, @start_time, @end_time), @status, @error_message);
        PRINT '[SUCCESS] Audit Log Inserted for Failed Operation.';


        -- Re-throw the error to the caller if needed
        -- THROW;

    END CATCH

    -- Restore SET NOCOUNT default behavior
    SET NOCOUNT OFF;
END;
GO