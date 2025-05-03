/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Author: Sneh Sparsh
Project Name: CRM ERP Data Warehouse Project
Created Date: 10-02-2025
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files.
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from CSV files to bronze tables.
    - Logs the row counts before truncating, after truncating, and after loading.
    - Inserts audit logs into the etl_audit_log table for tracking ETL operations.

Parameters:
    None.
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;

Dependencies:
    - bronze schema with target tables (crm_cust_info, crm_prd_info, crm_sales_details,
      erp_loc_a101, erp_cust_az12, erp_px_cat_g1v2).
    - CSV files located at the specified base path.
	- BULK INSERT requires specific file system permissions on the SQL Server machine.
    - etl_audit_log table with columns:
        (procedure_name NVARCHAR(255), table_name NVARCHAR(255),
         start_time DATETIME, end_time DATETIME,
         rows_before INT, rows_deleted INT, rows_after INT,
         load_duration INT, status NVARCHAR(50), error_message NVARCHAR(MAX))

===============================================================================
Script Update History:
| Name        | Update Date | Comments                                                                 |
|-------------|-------------|--------------------------------------------------------------------------|
| Sneh Sparsh | 14-02-2025  | Added variable to capture rows before load, deleted rows, and loaded rows to print in logs |
| Sneh Sparsh | 15-02-2025  | Added logic to insert all the logs in ETL Audit Log table for each source |
| Sneh Sparsh | 28-04-2025  | Enhanced PRINT messages for clearer output. Added SET NOCOUNT ON/OFF. Made base file path dynamic using a variable. |
===============================================================================

*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    -- Suppress the automatic row count messages for cleaner output
    SET NOCOUNT ON;

    -- Declare variables for logging and process control
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
    DECLARE @row_count_before INT, @row_count_after INT, @deleted_count INT;
    DECLARE @procedure_name NVARCHAR(255) = 'bronze.load_bronze';
    DECLARE @table_name NVARCHAR(255);
    DECLARE @status NVARCHAR(50);
    DECLARE @error_message NVARCHAR(MAX);
    DECLARE @base_file_path NVARCHAR(MAX) = 'C:\Users\934176\OneDrive - Cognizant\Desktop\Projects\sql-erp-crm-dwh\datasets\'; -- Base directory for CSV files
    DECLARE @file_name NVARCHAR(255); -- Variable for the specific file name
    DECLARE @full_file_path NVARCHAR(MAX); -- Variable for the constructed full file path
    DECLARE @sql_bulk_insert NVARCHAR(MAX); -- Variable to hold dynamic SQL for BULK INSERT

    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '====================================================================';
        PRINT '[INFO] ETL Process Started: Loading Bronze Layer';
        PRINT '[INFO] Start Time (Batch): ' + CONVERT(NVARCHAR, @batch_start_time, 120); -- ISO 8601 format
        PRINT '[INFO] Base File Path: ' + @base_file_path;
        PRINT '====================================================================';
        PRINT ''; -- Add a blank line for readability

        PRINT '--------------------------------------------------------------------';
        PRINT '[INFO] Starting CRM Tables Load from Source Files';
        PRINT '--------------------------------------------------------------------';
        PRINT '';

        -- CRM Customer Info
        SET @table_name = 'CRM_ERP_DWH.bronze.crm_cust_info';
        SET @file_name = 'source_crm\cust_info.csv'; -- Specific file name for this table
        SET @full_file_path = @base_file_path + @file_name; -- Construct full path
        SET @start_time = GETDATE();
        PRINT '[INFO] Processing Table: ' + @table_name;
        PRINT '[INFO] Source File: ' + @full_file_path;
        PRINT '[INFO] Start Time: ' + CONVERT(NVARCHAR, @start_time, 120);

        SELECT @row_count_before = COUNT(*) FROM CRM_ERP_DWH.bronze.crm_cust_info;
        PRINT '[INFO] Rows Before Truncate: ' + CAST(@row_count_before AS NVARCHAR);

        PRINT '[INFO] Truncating Table: ' + @table_name;
        TRUNCATE TABLE CRM_ERP_DWH.bronze.crm_cust_info;
        SET @deleted_count = @row_count_before; -- For truncate, deleted count is same as count before
        PRINT '[SUCCESS] Table Truncated. Deleted Count: ' + CAST(@deleted_count AS NVARCHAR);

        PRINT '[INFO] Bulk Inserting Data Into: ' + @table_name;
        -- Construct and execute BULK INSERT dynamically
        SET @sql_bulk_insert = 'BULK INSERT ' + @table_name + ' FROM ''' + @full_file_path + ''' WITH ( FIRSTROW = 2, FIELDTERMINATOR = '','', TABLOCK );';
        EXEC sp_executesql @sql_bulk_insert;

        SELECT @row_count_after = COUNT(*) FROM CRM_ERP_DWH.bronze.crm_cust_info;
        PRINT '[SUCCESS] Data Loaded. Rows Loaded: ' + CAST(@row_count_after AS NVARCHAR);
        SET @end_time = GETDATE();
        PRINT '[INFO] Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        PRINT '[INFO] Logging Audit Entry for ' + @table_name + ' (Status: Success)';
        INSERT INTO etl_audit_log (procedure_name, table_name, start_time, end_time, rows_before, rows_deleted, rows_after, load_duration, status)
        VALUES (@procedure_name, @table_name, @start_time, @end_time, @row_count_before, @deleted_count, @row_count_after, DATEDIFF(SECOND, @start_time, @end_time), 'Success');
        PRINT '[SUCCESS] Audit Log Inserted for ' + @table_name;
        PRINT '--------------------------------------------------------------------';
        PRINT '[INFO] Finished Processing Table: ' + @table_name;
        PRINT '[INFO] End Time: ' + CONVERT(NVARCHAR, @end_time, 120);
        PRINT '--------------------------------------------------------------------';
        PRINT ''; -- Add a blank line between tables


        -- CRM Product Info
        SET @table_name = 'CRM_ERP_DWH.bronze.crm_prd_info';
        SET @file_name = 'source_crm\prd_info.csv'; -- Specific file name for this table
        SET @full_file_path = @base_file_path + @file_name; -- Construct full path
        SET @start_time = GETDATE();
        PRINT '[INFO] Processing Table: ' + @table_name;
        PRINT '[INFO] Source File: ' + @full_file_path;
        PRINT '[INFO] Start Time: ' + CONVERT(NVARCHAR, @start_time, 120);

        SELECT @row_count_before = COUNT(*) FROM CRM_ERP_DWH.bronze.crm_prd_info;
        PRINT '[INFO] Rows Before Truncate: ' + CAST(@row_count_before AS NVARCHAR);

        PRINT '[INFO] Truncating Table: ' + @table_name;
        TRUNCATE TABLE CRM_ERP_DWH.bronze.crm_prd_info;
        SET @deleted_count = @row_count_before;
        PRINT '[SUCCESS] Table Truncated. Deleted Count: ' + CAST(@deleted_count AS NVARCHAR);

        PRINT '[INFO] Bulk Inserting Data Into: ' + @table_name;
        -- Construct and execute BULK INSERT dynamically
        SET @sql_bulk_insert = 'BULK INSERT ' + @table_name + ' FROM ''' + @full_file_path + ''' WITH ( FIRSTROW = 2, FIELDTERMINATOR = '','', TABLOCK );';
        EXEC sp_executesql @sql_bulk_insert;

        SELECT @row_count_after = COUNT(*) FROM CRM_ERP_DWH.bronze.crm_prd_info;
        PRINT '[SUCCESS] Data Loaded. Rows Loaded: ' + CAST(@row_count_after AS NVARCHAR);
        SET @end_time = GETDATE();
        PRINT '[INFO] Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        PRINT '[INFO] Logging Audit Entry for ' + @table_name + ' (Status: Success)';
        INSERT INTO etl_audit_log (procedure_name, table_name, start_time, end_time, rows_before, rows_deleted, rows_after, load_duration, status)
        VALUES (@procedure_name, @table_name, @start_time, @end_time, @row_count_before, @deleted_count, @row_count_after, DATEDIFF(SECOND, @start_time, @end_time), 'Success');
        PRINT '[SUCCESS] Audit Log Inserted for ' + @table_name;
        PRINT '--------------------------------------------------------------------';
        PRINT '[INFO] Finished Processing Table: ' + @table_name;
        PRINT '[INFO] End Time: ' + CONVERT(NVARCHAR, @end_time, 120);
        PRINT '--------------------------------------------------------------------';
        PRINT ''; -- Add a blank line between tables


        -- CRM Sales Details
        SET @table_name = 'CRM_ERP_DWH.bronze.crm_sales_details';
        SET @file_name = 'source_crm\sales_details.csv'; -- Specific file name for this table
        SET @full_file_path = @base_file_path + @file_name; -- Construct full path
        SET @start_time = GETDATE();
        PRINT '[INFO] Processing Table: ' + @table_name;
        PRINT '[INFO] Source File: ' + @full_file_path;
        PRINT '[INFO] Start Time: ' + CONVERT(NVARCHAR, @start_time, 120);

        SELECT @row_count_before = COUNT(*) FROM CRM_ERP_DWH.bronze.crm_sales_details;
        PRINT '[INFO] Rows Before Truncate: ' + CAST(@row_count_before AS NVARCHAR);

        PRINT '[INFO] Truncating Table: ' + @table_name;
        TRUNCATE TABLE CRM_ERP_DWH.bronze.crm_sales_details;
        SET @deleted_count = @row_count_before;
        PRINT '[SUCCESS] Table Truncated. Deleted Count: ' + CAST(@deleted_count AS NVARCHAR);

        PRINT '[INFO] Bulk Inserting Data Into: ' + @table_name;
        -- Construct and execute BULK INSERT dynamically
        SET @sql_bulk_insert = 'BULK INSERT ' + @table_name + ' FROM ''' + @full_file_path + ''' WITH ( FIRSTROW = 2, FIELDTERMINATOR = '','', TABLOCK );';
        EXEC sp_executesql @sql_bulk_insert;

        SELECT @row_count_after = COUNT(*) FROM CRM_ERP_DWH.bronze.crm_sales_details;
        PRINT '[SUCCESS] Data Loaded. Rows Loaded: ' + CAST(@row_count_after AS NVARCHAR);
        SET @end_time = GETDATE();
        PRINT '[INFO] Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        PRINT '[INFO] Logging Audit Entry for ' + @table_name + ' (Status: Success)';
        INSERT INTO etl_audit_log (procedure_name, table_name, start_time, end_time, rows_before, rows_deleted, rows_after, load_duration, status)
        VALUES (@procedure_name, @table_name, @start_time, @end_time, @row_count_before, @deleted_count, @row_count_after, DATEDIFF(SECOND, @start_time, @end_time), 'Success');
        PRINT '[SUCCESS] Audit Log Inserted for ' + @table_name;
        PRINT '--------------------------------------------------------------------';
        PRINT '[INFO] Finished Processing Table: ' + @table_name;
        PRINT '[INFO] End Time: ' + CONVERT(NVARCHAR, @end_time, 120);
        PRINT '--------------------------------------------------------------------';
        PRINT ''; -- Add a blank line between tables


        PRINT '--------------------------------------------------------------------';
        PRINT '[INFO] Starting ERP Tables Load from Source Files';
        PRINT '--------------------------------------------------------------------';
        PRINT '';


        -- ERP Location A101
        SET @table_name = 'CRM_ERP_DWH.bronze.erp_loc_a101';
        SET @file_name = 'source_erp\loc_a101.csv'; -- Specific file name for this table
        SET @full_file_path = @base_file_path + @file_name; -- Construct full path
        SET @start_time = GETDATE();
        PRINT '[INFO] Processing Table: ' + @table_name;
        PRINT '[INFO] Source File: ' + @full_file_path;
        PRINT '[INFO] Start Time: ' + CONVERT(NVARCHAR, @start_time, 120);

        SELECT @row_count_before = COUNT(*) FROM CRM_ERP_DWH.bronze.erp_loc_a101;
        PRINT '[INFO] Rows Before Truncate: ' + CAST(@row_count_before AS NVARCHAR);

        PRINT '[INFO] Truncating Table: ' + @table_name;
        TRUNCATE TABLE CRM_ERP_DWH.bronze.erp_loc_a101;
        SET @deleted_count = @row_count_before;
        PRINT '[SUCCESS] Table Truncated. Deleted Count: ' + CAST(@deleted_count AS NVARCHAR);

        PRINT '[INFO] Bulk Inserting Data Into: ' + @table_name;
        -- Construct and execute BULK INSERT dynamically
        SET @sql_bulk_insert = 'BULK INSERT ' + @table_name + ' FROM ''' + @full_file_path + ''' WITH ( FIRSTROW = 2, FIELDTERMINATOR = '','', TABLOCK );';
        EXEC sp_executesql @sql_bulk_insert;

        SELECT @row_count_after = COUNT(*) FROM CRM_ERP_DWH.bronze.erp_loc_a101;
        PRINT '[SUCCESS] Data Loaded. Rows Loaded: ' + CAST(@row_count_after AS NVARCHAR);
        SET @end_time = GETDATE();
        PRINT '[INFO] Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        PRINT '[INFO] Logging Audit Entry for ' + @table_name + ' (Status: Success)';
        INSERT INTO etl_audit_log (procedure_name, table_name, start_time, end_time, rows_before, rows_deleted, rows_after, load_duration, status)
        VALUES (@procedure_name, @table_name, @start_time, @end_time, @row_count_before, @deleted_count, @row_count_after, DATEDIFF(SECOND, @start_time, @end_time), 'Success');
        PRINT '[SUCCESS] Audit Log Inserted for ' + @table_name;
        PRINT '--------------------------------------------------------------------';
        PRINT '[INFO] Finished Processing Table: ' + @table_name;
        PRINT '[INFO] End Time: ' + CONVERT(NVARCHAR, @end_time, 120);
        PRINT '--------------------------------------------------------------------';
        PRINT ''; -- Add a blank line between tables


        -- ERP Customer AZ12
        SET @table_name = 'CRM_ERP_DWH.bronze.erp_cust_az12';
        SET @file_name = 'source_erp\cust_az12.csv'; -- Specific file name for this table
        SET @full_file_path = @base_file_path + @file_name; -- Construct full path
        SET @start_time = GETDATE();
        PRINT '[INFO] Processing Table: ' + @table_name;
        PRINT '[INFO] Source File: ' + @full_file_path;
        PRINT '[INFO] Start Time: ' + CONVERT(NVARCHAR, @start_time, 120);

        SELECT @row_count_before = COUNT(*) FROM CRM_ERP_DWH.bronze.erp_cust_az12;
        PRINT '[INFO] Rows Before Truncate: ' + CAST(@row_count_before AS NVARCHAR);

        PRINT '[INFO] Truncating Table: ' + @table_name;
        TRUNCATE TABLE CRM_ERP_DWH.bronze.erp_cust_az12;
        SET @deleted_count = @row_count_before;
        PRINT '[SUCCESS] Table Truncated. Deleted Count: ' + CAST(@deleted_count AS NVARCHAR);

        PRINT '[INFO] Bulk Inserting Data Into: ' + @table_name;
        -- Construct and execute BULK INSERT dynamically
        SET @sql_bulk_insert = 'BULK INSERT ' + @table_name + ' FROM ''' + @full_file_path + ''' WITH ( FIRSTROW = 2, FIELDTERMINATOR = '','', TABLOCK );';
        EXEC sp_executesql @sql_bulk_insert;

        SELECT @row_count_after = COUNT(*) FROM CRM_ERP_DWH.bronze.erp_cust_az12;
        PRINT '[SUCCESS] Data Loaded. Rows Loaded: ' + CAST(@row_count_after AS NVARCHAR);
        SET @end_time = GETDATE();
        PRINT '[INFO] Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        PRINT '[INFO] Logging Audit Entry for ' + @table_name + ' (Status: Success)';
        INSERT INTO etl_audit_log (procedure_name, table_name, start_time, end_time, rows_before, rows_deleted, rows_after, load_duration, status)
        VALUES (@procedure_name, @table_name, @start_time, @end_time, @row_count_before, @deleted_count, @row_count_after, DATEDIFF(SECOND, @start_time, @end_time), 'Success');
        PRINT '[SUCCESS] Audit Log Inserted for ' + @table_name;
        PRINT '--------------------------------------------------------------------';
        PRINT '[INFO] Finished Processing Table: ' + @table_name;
        PRINT '[INFO] End Time: ' + CONVERT(NVARCHAR, @end_time, 120);
        PRINT '--------------------------------------------------------------------';
        PRINT ''; -- Add a blank line between tables


        -- ERP PX Category G1V2
        SET @table_name = 'CRM_ERP_DWH.bronze.erp_px_cat_g1v2';
        SET @file_name = 'source_erp\px_cat_g1v2.csv'; -- Specific file name for this table
        SET @full_file_path = @base_file_path + @file_name; -- Construct full path
        SET @start_time = GETDATE();
        PRINT '[INFO] Processing Table: ' + @table_name;
        PRINT '[INFO] Source File: ' + @full_file_path;
        PRINT '[INFO] Start Time: ' + CONVERT(NVARCHAR, @start_time, 120);

        SELECT @row_count_before = COUNT(*) FROM CRM_ERP_DWH.bronze.erp_px_cat_g1v2;
        PRINT '[INFO] Rows Before Truncate: ' + CAST(@row_count_before AS NVARCHAR);

        PRINT '[INFO] Truncating Table: ' + @table_name;
        TRUNCATE TABLE CRM_ERP_DWH.bronze.erp_px_cat_g1v2;
        SET @deleted_count = @row_count_before;
        PRINT '[SUCCESS] Table Truncated. Deleted Count: ' + CAST(@deleted_count AS NVARCHAR);

        PRINT '[INFO] Bulk Inserting Data Into: ' + @table_name;
        -- Construct and execute BULK INSERT dynamically
        SET @sql_bulk_insert = 'BULK INSERT ' + @table_name + ' FROM ''' + @full_file_path + ''' WITH ( FIRSTROW = 2, FIELDTERMINATOR = '','', TABLOCK );';
        EXEC sp_executesql @sql_bulk_insert;

        SELECT @row_count_after = COUNT(*) FROM CRM_ERP_DWH.bronze.erp_px_cat_g1v2;
        PRINT '[SUCCESS] Data Loaded. Rows Loaded: ' + CAST(@row_count_after AS NVARCHAR);
        SET @end_time = GETDATE();
        PRINT '[INFO] Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        PRINT '[INFO] Logging Audit Entry for ' + @table_name + ' (Status: Success)';
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
        PRINT '[SUCCESS] ETL Process Completed: Loading Bronze Layer Finished';
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
        PRINT '[ERROR] ETL Process Aborted: Error Loading Bronze Layer';
        PRINT '[ERROR] Error Time: ' + CONVERT(NVARCHAR, GETDATE(), 120);
        PRINT '[ERROR] Error Message: ' + @error_message;
        PRINT '[ERROR] Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT '[ERROR] Error State: ' + CAST(ERROR_STATE() AS NVARCHAR);
        -- Note: @table_name here will be the table being processed when the error occurred
        PRINT '[ERROR] Failed Table (Approximate): ' + ISNULL(@table_name, 'N/A - Before first table');
        -- Note: @full_file_path might hold the path being used if the error happened during BULK INSERT
        PRINT '[ERROR] Source File (Approximate): ' + ISNULL(@full_file_path, 'N/A - Before first file');
        PRINT '====================================================================';

        -- Log the failure for the last table/file being processed (or the overall procedure if before any table)
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
