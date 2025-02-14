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
===============================================================================
Script Update History:
| Name        | Update Date | Comments                                                                 |
|-------------|-------------|--------------------------------------------------------------------------|
| Sneh Sparsh | 14-02-2025  | Added variable to capture rows before load, deleted rows, and loaded rows to print in logs |
| Sneh Sparsh | 14-02-2025  | Added logic to insert all the logs in ETL Audit Log table for each source |
===============================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
    DECLARE @row_count_before INT, @row_count_after INT, @deleted_count INT;
    DECLARE @procedure_name NVARCHAR(255) = 'bronze.load_bronze';
    DECLARE @table_name NVARCHAR(255);
    DECLARE @status NVARCHAR(50);
    DECLARE @error_message NVARCHAR(MAX);

    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT 'Loading Bronze Layer';
        PRINT '================================================';

        PRINT '------------------------------------------------';
        PRINT 'Loading CRM Tables';
        PRINT '------------------------------------------------';

        -- CRM Customer Info
        SET @table_name = 'CRM_ERP_DWH.bronze.crm_cust_info';
        SET @start_time = GETDATE();
        SELECT @row_count_before = COUNT(*) FROM CRM_ERP_DWH.bronze.crm_cust_info;
        PRINT '>> Count of Rows Before Truncate: ' + CAST(@row_count_before AS NVARCHAR);
        TRUNCATE TABLE CRM_ERP_DWH.bronze.crm_cust_info;
        SELECT @deleted_count = @row_count_before;
        PRINT '>> Count of Deleted Records After Truncate: ' + CAST(@deleted_count AS NVARCHAR);
        BULK INSERT CRM_ERP_DWH.bronze.crm_cust_info
        FROM 'C:\Users\934176\OneDrive - Cognizant\Desktop\Projects\sql-erp-crm-dwh\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SELECT @row_count_after = COUNT(*) FROM CRM_ERP_DWH.bronze.crm_cust_info;
        PRINT '>> Count of Rows After Load: ' + CAST(@row_count_after AS NVARCHAR);
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';
        PRINT '>> Inserting Audit Log for CRM Customer Info';
        INSERT INTO etl_audit_log (procedure_name, table_name, start_time, end_time, rows_before, rows_deleted, rows_after, load_duration, status)
        VALUES (@procedure_name, @table_name, @start_time, @end_time, @row_count_before, @deleted_count, @row_count_after, DATEDIFF(SECOND, @start_time, @end_time), 'Success');
        PRINT '>> Audit Log Inserted for CRM Customer Info';

        -- CRM Product Info
        SET @table_name = 'CRM_ERP_DWH.bronze.crm_prd_info';
        SET @start_time = GETDATE();
        SELECT @row_count_before = COUNT(*) FROM CRM_ERP_DWH.bronze.crm_prd_info;
        PRINT '>> Count of Rows Before Truncate: ' + CAST(@row_count_before AS NVARCHAR);
        TRUNCATE TABLE CRM_ERP_DWH.bronze.crm_prd_info;
        SELECT @deleted_count = @row_count_before;
        PRINT '>> Count of Deleted Records After Truncate: ' + CAST(@deleted_count AS NVARCHAR);
        BULK INSERT CRM_ERP_DWH.bronze.crm_prd_info
        FROM 'C:\Users\934176\OneDrive - Cognizant\Desktop\Projects\sql-erp-crm-dwh\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SELECT @row_count_after = COUNT(*) FROM CRM_ERP_DWH.bronze.crm_prd_info;
        PRINT '>> Count of Rows After Load: ' + CAST(@row_count_after AS NVARCHAR);
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';
        PRINT '>> Inserting Audit Log for CRM Product Info';
        INSERT INTO etl_audit_log (procedure_name, table_name, start_time, end_time, rows_before, rows_deleted, rows_after, load_duration, status)
        VALUES (@procedure_name, @table_name, @start_time, @end_time, @row_count_before, @deleted_count, @row_count_after, DATEDIFF(SECOND, @start_time, @end_time), 'Success');
        PRINT '>> Audit Log Inserted for CRM Product Info';

        -- CRM Sales Details
        SET @table_name = 'CRM_ERP_DWH.bronze.crm_sales_details';
        SET @start_time = GETDATE();
        SELECT @row_count_before = COUNT(*) FROM CRM_ERP_DWH.bronze.crm_sales_details;
        PRINT '>> Count of Rows Before Truncate: ' + CAST(@row_count_before AS NVARCHAR);
        TRUNCATE TABLE CRM_ERP_DWH.bronze.crm_sales_details;
        SELECT @deleted_count = @row_count_before;
        PRINT '>> Count of Deleted Records After Truncate: ' + CAST(@deleted_count AS NVARCHAR);
        BULK INSERT CRM_ERP_DWH.bronze.crm_sales_details
        FROM 'C:\Users\934176\OneDrive - Cognizant\Desktop\Projects\sql-erp-crm-dwh\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SELECT @row_count_after = COUNT(*) FROM CRM_ERP_DWH.bronze.crm_sales_details;
        PRINT '>> Count of Rows After Load: ' + CAST(@row_count_after AS NVARCHAR);
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';
        PRINT '>> Inserting Audit Log for CRM Sales Details';
        INSERT INTO etl_audit_log (procedure_name, table_name, start_time, end_time, rows_before, rows_deleted, rows_after, load_duration, status)
        VALUES (@procedure_name, @table_name, @start_time, @end_time, @row_count_before, @deleted_count, @row_count_after, DATEDIFF(SECOND, @start_time, @end_time), 'Success');
        PRINT '>> Audit Log Inserted for CRM Sales Details';

        PRINT '------------------------------------------------';
        PRINT 'Loading ERP Tables';
        PRINT '------------------------------------------------';

        -- ERP Location A101
        SET @table_name = 'CRM_ERP_DWH.bronze.erp_loc_a101';
        SET @start_time = GETDATE();
        SELECT @row_count_before = COUNT(*) FROM CRM_ERP_DWH.bronze.erp_loc_a101;
        PRINT '>> Count of Rows Before Truncate: ' + CAST(@row_count_before AS NVARCHAR);
        TRUNCATE TABLE CRM_ERP_DWH.bronze.erp_loc_a101;
        SELECT @deleted_count = @row_count_before;
        PRINT '>> Count of Deleted Records After Truncate: ' + CAST(@deleted_count AS NVARCHAR);
        BULK INSERT CRM_ERP_DWH.bronze.erp_loc_a101
        FROM 'C:\Users\934176\OneDrive - Cognizant\Desktop\Projects\sql-erp-crm-dwh\datasets\source_erp\loc_a101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SELECT @row_count_after = COUNT(*) FROM CRM_ERP_DWH.bronze.erp_loc_a101;
        PRINT '>> Count of Rows After Load: ' + CAST(@row_count_after AS NVARCHAR);
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';
        PRINT '>> Inserting Audit Log for ERP Location A101';
        INSERT INTO etl_audit_log (procedure_name, table_name, start_time, end_time, rows_before, rows_deleted, rows_after, load_duration, status)
        VALUES (@procedure_name, @table_name, @start_time, @end_time, @row_count_before, @deleted_count, @row_count_after, DATEDIFF(SECOND, @start_time, @end_time), 'Success');
        PRINT '>> Audit Log Inserted for ERP Location A101';

        -- ERP Customer AZ12
        SET @table_name = 'CRM_ERP_DWH.bronze.erp_cust_az12';
        SET @start_time = GETDATE();
        SELECT @row_count_before = COUNT(*) FROM CRM_ERP_DWH.bronze.erp_cust_az12;
        PRINT '>> Count of Rows Before Truncate: ' + CAST(@row_count_before AS NVARCHAR);
        TRUNCATE TABLE CRM_ERP_DWH.bronze.erp_cust_az12;
        SELECT @deleted_count = @row_count_before;
        PRINT '>> Count of Deleted Records After Truncate: ' + CAST(@deleted_count AS NVARCHAR);
        BULK INSERT CRM_ERP_DWH.bronze.erp_cust_az12
        FROM 'C:\Users\934176\OneDrive - Cognizant\Desktop\Projects\sql-erp-crm-dwh\datasets\source_erp\cust_az12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SELECT @row_count_after = COUNT(*) FROM CRM_ERP_DWH.bronze.erp_cust_az12;
        PRINT '>> Count of Rows After Load: ' + CAST(@row_count_after AS NVARCHAR);
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';
        PRINT '>> Inserting Audit Log for ERP Customer AZ12';
        INSERT INTO etl_audit_log (procedure_name, table_name, start_time, end_time, rows_before, rows_deleted, rows_after, load_duration, status)
        VALUES (@procedure_name, @table_name, @start_time, @end_time, @row_count_before, @deleted_count, @row_count_after, DATEDIFF(SECOND, @start_time, @end_time), 'Success');
        PRINT '>> Audit Log Inserted for ERP Customer AZ12';

        -- ERP PX Category G1V2
        SET @table_name = 'CRM_ERP_DWH.bronze.erp_px_cat_g1v2';
        SET @start_time = GETDATE();
        SELECT @row_count_before = COUNT(*) FROM CRM_ERP_DWH.bronze.erp_px_cat_g1v2;
        PRINT '>> Count of Rows Before Truncate: ' + CAST(@row_count_before AS NVARCHAR);
        TRUNCATE TABLE CRM_ERP_DWH.bronze.erp_px_cat_g1v2;
        SELECT @deleted_count = @row_count_before;
        PRINT '>> Count of Deleted Records After Truncate: ' + CAST(@deleted_count AS NVARCHAR);
        BULK INSERT CRM_ERP_DWH.bronze.erp_px_cat_g1v2
        FROM 'C:\Users\934176\OneDrive - Cognizant\Desktop\Projects\sql-erp-crm-dwh\datasets\source_erp\px_cat_g1v2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SELECT @row_count_after = COUNT(*) FROM CRM_ERP_DWH.bronze.erp_px_cat_g1v2;
        PRINT '>> Count of Rows After Load: ' + CAST(@row_count_after AS NVARCHAR);
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';
        PRINT '>> Inserting Audit Log for ERP PX Category G1V2';
        INSERT INTO etl_audit_log (procedure_name, table_name, start_time, end_time, rows_before, rows_deleted, rows_after, load_duration, status)
        VALUES (@procedure_name, @table_name, @start_time, @end_time, @row_count_before, @deleted_count, @row_count_after, DATEDIFF(SECOND, @start_time, @end_time), 'Success');
        PRINT '>> Audit Log Inserted for ERP PX Category G1V2';

        SET @batch_end_time = GETDATE();
        PRINT '==========================================';
        PRINT 'Loading Bronze Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
        PRINT '==========================================';
    END TRY
    BEGIN CATCH
        SET @end_time = GETDATE();
        SET @error_message = ERROR_MESSAGE();
        PRINT '==========================================';
        PRINT 'ERROR OCCURRED DURING LOADING BRONZE LAYER';
        PRINT 'Error Message: ' + @error_message;
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '==========================================';
        INSERT INTO etl_audit_log (procedure_name, table_name, start_time, end_time, rows_before, rows_deleted, rows_after, load_duration, status, error_message)
        VALUES (@procedure_name, @table_name, @start_time, @end_time, @row_count_before, @deleted_count, @row_count_after, DATEDIFF(SECOND, @start_time, @end_time), 'Failed', @error_message);
        PRINT '>> Audit Log Inserted for Failed Operation';
    END CATCH
END;
