/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/

CREATE OR ALTER PROCEDURE CRM_ERP_DWH.bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: CRM_ERP_DWH.bronze.crm_cust_info';
		TRUNCATE TABLE CRM_ERP_DWH.bronze.crm_cust_info;
		PRINT '>> Inserting Data Into: CRM_ERP_DWH.bronze.crm_cust_info';
		BULK INSERT CRM_ERP_DWH.bronze.crm_cust_info
		FROM '/Users/snehsparsh/Desktop/workspace/ADS/sql-erp-crm-dwh/datasets/source_crm/cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: CRM_ERP_DWH.bronze.crm_prd_info';
		TRUNCATE TABLE CRM_ERP_DWH.bronze.crm_prd_info;

		PRINT '>> Inserting Data Into: CRM_ERP_DWH.bronze.crm_prd_info';
		BULK INSERT CRM_ERP_DWH.bronze.crm_prd_info
		FROM 'sql-erp-crm-dwh\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: CRM_ERP_DWH.bronze.crm_sales_details';
		TRUNCATE TABLE CRM_ERP_DWH.bronze.crm_sales_details;
		PRINT '>> Inserting Data Into: CRM_ERP_DWH.bronze.crm_sales_details';
		BULK INSERT CRM_ERP_DWH.bronze.crm_sales_details
		FROM 'sql-erp-crm-dwh\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		PRINT '------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------------';
		
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: CRM_ERP_DWH.bronze.erp_loc_a101';
		TRUNCATE TABLE CRM_ERP_DWH.bronze.erp_loc_a101;
		PRINT '>> Inserting Data Into: CRM_ERP_DWH.bronze.erp_loc_a101';
		BULK INSERT CRM_ERP_DWH.bronze.erp_loc_a101
		FROM 'sql-erp-crm-dwh\datasets\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: CRM_ERP_DWH.bronze.erp_cust_az12';
		TRUNCATE TABLE CRM_ERP_DWH.bronze.erp_cust_az12;
		PRINT '>> Inserting Data Into: CRM_ERP_DWH.bronze.erp_cust_az12';
		BULK INSERT CRM_ERP_DWH.bronze.erp_cust_az12
		FROM 'sql-erp-crm-dwh\datasets\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: CRM_ERP_DWH.bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE CRM_ERP_DWH.bronze.erp_px_cat_g1v2;
		PRINT '>> Inserting Data Into: CRM_ERP_DWH.bronze.erp_px_cat_g1v2';
		BULK INSERT CRM_ERP_DWH.bronze.erp_px_cat_g1v2
		FROM 'sql-erp-crm-dwh\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Bronze Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
	END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
END
