

------------------------------------------------------------
-- CRM Tables
------------------------------------------------------------

SELECT TOP 1000 * FROM CRM_ERP_DWH.bronze.crm_cust_info;

SELECT COUNT(1) AS tot_cnt, COUNT(DISTINCT cst_id) as cst_id_cnt, COUNT(DISTINCT cst_key) AS cst_key_cnt 
FROM CRM_ERP_DWH.bronze.crm_cust_info;

SELECT TOP 1000 * FROM CRM_ERP_DWH.bronze.crm_prd_info;

SELECT * FROM CRM_ERP_DWH.bronze.crm_prd_info WHERE prd_key = 'AC-HE-HL-U509-B';

SELECT COUNT(1) AS tot_cnt, COUNT( DISTINCT prd_id) AS prd_id_cnt, COUNT(DISTINCT prd_key) AS prd_key_cnt
FROM CRM_ERP_DWH.bronze.crm_prd_info;

SELECT TOP 1000 * FROM CRM_ERP_DWH.bronze.crm_sales_details;

SELECT * FROM CRM_ERP_DWH.bronze.crm_sales_details WHERE sls_prd_key = 'BK-M82S-44';

------------------------------------------------------------
-- ERP Tables
------------------------------------------------------------

SELECT TOP 1000 * FROM CRM_ERP_DWH.bronze.erp_cust_az12;
SELECT TOP 1000 * FROM CRM_ERP_DWH.bronze.crm_cust_info;

SELECT TOP 1000 * FROM CRM_ERP_DWH.bronze.erp_loc_a101;
SELECT TOP 1000 * FROM CRM_ERP_DWH.bronze.crm_cust_info;

SELECT TOP 1000 * FROM CRM_ERP_DWH.bronze.erp_px_cat_g1v2;
SELECT TOP 1000 * FROM CRM_ERP_DWH.bronze.crm_prd_info;




/*
=============================================================
CRM Customer Info
=============================================================
*/

------------------------------------------------------------
-- Integerity Check
------------------------------------------------------------

-- Check for Nulls or Duplicates in primary key
-- Expectation : No Results

SELECT *
FROM CRM_ERP_DWH.bronze.crm_cust_info;

SELECT COUNT(1) AS tot_cnt,
       COUNT(DISTINCT cst_id) AS cst_id_cnt,
       COUNT(DISTINCT cst_key) AS cst_key_cnt,
       COUNT(DISTINCT CONCAT(cst_key, '-', cst_id)) AS cst_key_cst_id_cnt
FROM CRM_ERP_DWH.bronze.crm_cust_info;


SELECT cst_id,
       COUNT(1) AS tot_cnt
FROM CRM_ERP_DWH.bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Other Analysis
SELECT * 
FROM CRM_ERP_DWH.bronze.crm_cust_info
WHERE cst_id IS NULL;

SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last 
FROM CRM_ERP_DWH.bronze.crm_cust_info
WHERE cst_id = 29466;

-- Solution : Using Row Number to pick only latest record from source 
SELECT * 
FROM (
	SELECT * , ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
	FROM CRM_ERP_DWH.bronze.crm_cust_info
	) t
WHERE flag_last = 1;

------------------------------------------------------------
-- Quality Check
------------------------------------------------------------

-- Check for unwanted spaces
-- Expectation : No Results
SELECT cst_firstname
FROM CRM_ERP_DWH.bronze.crm_cust_info
WHERE cst_firstname <> TRIM(cst_firstname);

-- Check for unwanted spaces
-- Expectation : No Results
SELECT cst_lastname
FROM CRM_ERP_DWH.bronze.crm_cust_info
WHERE cst_lastname <> TRIM(cst_lastname);

-- Check for unwanted spaces
-- Expectation : No Results
SELECT cst_gndr
FROM CRM_ERP_DWH.bronze.crm_cust_info
WHERE cst_gndr <> TRIM(cst_gndr);

-- Count of unwanted spaces in each column
SELECT 'cst_firstname' AS column_name, COUNT(*) AS count_of_unwanted_spaces
FROM CRM_ERP_DWH.bronze.crm_cust_info
WHERE cst_firstname <> TRIM(cst_firstname)
UNION ALL
SELECT 'cst_lastname' AS column_name, COUNT(*) AS count_of_unwanted_spaces
FROM CRM_ERP_DWH.bronze.crm_cust_info
WHERE cst_lastname <> TRIM(cst_lastname)
UNION ALL
SELECT 'cst_gndr' AS column_name, COUNT(*) AS count_of_unwanted_spaces
FROM CRM_ERP_DWH.bronze.crm_cust_info
WHERE cst_gndr <> TRIM(cst_gndr)
UNION ALL
SELECT 'cst_marital_status' AS column_name, COUNT(*) AS count_of_unwanted_spaces
FROM CRM_ERP_DWH.bronze.crm_cust_info
WHERE cst_marital_status <> TRIM(cst_marital_status);

-- Solution: Add Trim while extracting Data
SELECT 
    cst_id,
    cst_key,
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname) AS cst_lastname ,
    cst_marital_status,
    cst_gndr,
    cst_create_date
FROM CRM_ERP_DWH.bronze.crm_cust_info;


------------------------------------------------------------
-- Cardinality Check
------------------------------------------------------------

-- Data Standardization & Consistency
SELECT DISTINCT cst_gndr
FROM CRM_ERP_DWH.bronze.crm_cust_info;

--cst_gndr: High Cardinality -> NULL, F, M

-- Solution : Candidate for more business friendly names
SELECT 
    cst_id,
    cst_key,
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname) AS cst_lastname ,
    cst_marital_status,
	CASE 
		WHEN cst_gndr = 'F' THEN 'Female'
		WHEN cst_gndr = 'M' THEN 'Male'
		ELSE 'n/a'
	END AS cst_gndr,
    cst_create_date
FROM CRM_ERP_DWH.bronze.crm_cust_info;

SELECT DISTINCT cst_marital_status
FROM CRM_ERP_DWH.bronze.crm_cust_info;

--cst_gndr: High Cardinality -> NULL, S, M

-- Solution : Candidate for more business friendly names
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
FROM CRM_ERP_DWH.bronze.crm_cust_info;


/*
=============================================================
CRM Product Info
=============================================================
*/
SELECT TOP (1000) [prd_id]
      ,[prd_key]
      ,[prd_nm]
      ,[prd_cost]
      ,[prd_line]
      ,[prd_start_dt]
      ,[prd_end_dt]
  FROM [CRM_ERP_DWH].[bronze].[crm_prd_info]

-- Deriving new column prd_key and cat_id from prd_key
SELECT prd_id
      ,prd_key
	  ,REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id
      ,prd_nm
      ,prd_cost
      ,prd_line
      ,prd_start_dt
      ,prd_end_dt
  FROM CRM_ERP_DWH.bronze.crm_prd_info
  WHERE REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') NOT IN (SELECT DISTINCT id FROM bronze.erp_px_cat_g1v2);

SELECT prd_id
      ,prd_key
	  ,REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id
	  ,SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key
      ,prd_nm
      ,prd_cost
      ,prd_line
      ,prd_start_dt
      ,prd_end_dt
  FROM CRM_ERP_DWH.bronze.crm_prd_info
  WHERE SUBSTRING(prd_key, 7, LEN(prd_key)) IN (SELECT sls_prd_key FROM CRM_ERP_DWH.bronze.crm_sales_details)

SELECT prd_id
      ,prd_key
	  ,REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id
	  ,SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key
      ,prd_nm
	  ,prd_cost
      ,prd_line
      ,prd_start_dt
      ,prd_end_dt
  FROM CRM_ERP_DWH.bronze.crm_prd_info;

------------------------------------------------------------
-- Integerity Check
------------------------------------------------------------

-- Check for Nulls or Duplicates in primary key
-- Expectation : No Results
SELECT prd_id,
       COUNT(1) AS tot_cnt
FROM CRM_ERP_DWH.bronze.[crm_prd_info]
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

------------------------------------------------------------
-- Quality Check
------------------------------------------------------------

-- Check for unwanted spaces
-- Expectation : No Results
SELECT prd_nm
FROM CRM_ERP_DWH.bronze.[crm_prd_info]
WHERE prd_nm <> TRIM(prd_nm);

-- Check for Nulls or Negative Numbers
-- Expectation : No Results
SELECT prd_cost
FROM CRM_ERP_DWH.bronze.[crm_prd_info]
WHERE prd_cost < 0 OR prd_cost IS NULL;

------------------------------------------------------------
-- Cardinality Check
------------------------------------------------------------

-- Data Standardization & Consistency
SELECT DISTINCT prd_line
FROM CRM_ERP_DWH.bronze.crm_prd_info;

-- prd_line: High Cardinality -> NULL, M, R, S, T

-- Solution : Candidate for more business friendly names

SELECT prd_id
      ,prd_key
	  ,REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id
	  ,SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key
      ,prd_nm
      ,ISNULL(prd_cost, 0) AS prd_cost
      ,prd_line
	  ,CASE UPPER(TRIM(prd_line)) 
			WHEN 'M' THEN 'Mountain'
			WHEN 'R' THEN 'Road'
			WHEN 'S' THEN 'Other Sales'
			WHEN 'T' THEN 'Touring'
			ELSE 'n/a'
	   END AS prd_line
      ,prd_start_dt
      ,prd_end_dt
  FROM CRM_ERP_DWH.bronze.crm_prd_info


-- Check for Invalid Date Orders
SELECT *
FROM CRM_ERP_DWH.bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

SELECT prd_id
      ,prd_key
	  ,REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id
	  ,SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key
      ,prd_nm
      ,ISNULL(prd_cost, 0) AS prd_cost
      ,prd_line
	  ,CASE UPPER(TRIM(prd_line)) 
			WHEN 'M' THEN 'Mountain'
			WHEN 'R' THEN 'Road'
			WHEN 'S' THEN 'Other Sales'
			WHEN 'T' THEN 'Touring'
			ELSE 'n/a'
	   END AS prd_line
      ,CAST (prd_start_dt AS DATE) AS prd_start_dt
	  ,CAST (LEAD(prd_start_dt, 1) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE) AS prd_end_dt
  FROM CRM_ERP_DWH.bronze.crm_prd_info
  WHERE prd_end_dt < prd_start_dt

/*
=============================================================
CRM Sales Details
=============================================================
*/
SELECT sls_ord_num
      ,sls_prd_key
      ,sls_cust_id
      ,sls_order_dt
      ,sls_ship_dt
      ,sls_due_dt
      ,sls_sales
      ,sls_quantity
      ,sls_price
  FROM CRM_ERP_DWH.bronze.crm_sales_details

------------------------------------------------------------
-- Quality Check
------------------------------------------------------------

-- Check for unwanted spaces
-- Expectation : No Results
SELECT sls_ord_num
FROM CRM_ERP_DWH.bronze.crm_sales_details
WHERE sls_ord_num <> TRIM(sls_ord_num);


-- Checking PK and FK relation between crm_prd_info and crm_sales_details
SELECT * 
FROM CRM_ERP_DWH.bronze.crm_sales_details 
WHERE sls_prd_key NOT IN (SELECT prd_key FROM CRM_ERP_DWH.silver.crm_prd_info)

-- Checking PK and FK relation between crm_prd_info and crm_cust_info
SELECT * 
FROM CRM_ERP_DWH.bronze.crm_sales_details 
WHERE sls_cust_id NOT IN (SELECT cst_id FROM CRM_ERP_DWH.silver.crm_cust_info)

-- Check for Invalid Date
SELECT 
sls_order_dt
FROM CRM_ERP_DWH.bronze.crm_sales_details
-- Checking for Negative numbers or zeros as can't be casted to date
WHERE sls_order_dt <= 0

-- Handling Null in Dates
SELECT 
NULLIF(sls_order_dt, 0) AS sls_order_dt
FROM CRM_ERP_DWH.bronze.crm_sales_details
WHERE sls_order_dt <= 0

-- Checking Dates Column should not have LENGTH > 8
SELECT 
NULLIF(sls_order_dt, 0) AS sls_order_dt
FROM CRM_ERP_DWH.bronze.crm_sales_details
WHERE LEN(sls_order_dt) <> 8

-- Check for outliers by validating the boundaries of the date range
SELECT 
NULLIF(sls_order_dt, 0) AS sls_order_dt
FROM CRM_ERP_DWH.bronze.crm_sales_details
WHERE sls_order_dt > 20500101
OR sls_order_dt < 19000101


-- Checking for other date columns
SELECT 
sls_ship_dt
FROM CRM_ERP_DWH.bronze.crm_sales_details
-- Checking for Negative numbers or zeros as can't be casted to date
WHERE sls_ship_dt <= 0 OR LEN(sls_ship_dt) <> 8

SELECT 
sls_due_dt
FROM CRM_ERP_DWH.bronze.crm_sales_details
-- Checking for Negative numbers or zeros as can't be casted to date
WHERE sls_due_dt <= 0 OR LEN(sls_due_dt) <> 8

-- Handling Dates with length <> 8
-- Converting Date column STRING to DATE data type
SELECT sls_ord_num
      ,sls_prd_key
      ,sls_cust_id
      ,CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) <> 8 THEN NULL
			ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
	   END AS sls_order_dt
	  ,CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) <> 8 THEN NULL
			ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
	   END AS sls_ship_dt
	  ,CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) <> 8 THEN NULL
			ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
	   END AS sls_due_dt
      ,sls_sales
      ,sls_quantity
      ,sls_price
  FROM CRM_ERP_DWH.bronze.crm_sales_details;

-- Checking for the Invalid Date 
-- Ideal Order : Order Date < Ship Date OR Due Date
SELECT *
FROM CRM_ERP_DWH.bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt 
OR sls_order_dt > sls_due_dt

-- Check for Business Rules
-- Sales = Quantity * Price
-- Sales, Quantity, Price can't be Negative, Zeros, or Nulls

SELECT
	sls_sales,
	sls_quantity,
	sls_price
FROM CRM_ERP_DWH.bronze.crm_sales_details
WHERE sls_sales <> (sls_quantity * sls_price)
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price

-- Rules to handle above analyzed bad data
-- 1. If Sales is negative, zero or null derive it using Quantity and Price : sls_sales = sls_quantity * sls_price
-- 2. If Price is zero or null, calculate it using Sales and Quantity : sls_price = sls_sales/sls_quantity
-- 3. If Price is negative, convert it to a positive value : sls_price * -1
SELECT
	sls_sales AS old_sls_sales,
	CASE 
		WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales <> sls_quantity * ABS(sls_price) 
		THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales
	END AS sls_sales,
	sls_price AS old_sls_price,
	CASE 
		WHEN sls_price IS NULL OR sls_price <= 0
		THEN sls_sales / NULLIF(sls_quantity, 0)
		ELSE sls_price
	END AS sls_price,
	sls_quantity
FROM CRM_ERP_DWH.bronze.crm_sales_details
WHERE sls_sales <> (sls_quantity * sls_price)
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price




/*
=============================================================
ERP Customer Info (erp_cust_az12)
=============================================================
*/

----------------------------------------------------------------------------------------------------
-- Object Name       : erp_cust_az12 (Bronze Layer)
-- Description       : Contains birth date information for ERP customers.
-- Use Case          : This table will be joined with the 'crm_cust_info' table
--                     in the CRM system to link customer details with their location.
-- Join Condition    : crm_cust_info.cst_key = erp_cust_az12.cid
----------------------------------------------------------------------------------------------------

----------------------------------------------------------------------------------------------------
-- Ad-hoc Analysis: Quick look at the top 1000 records in the bronze 'erp_cust_az12' table.
----------------------------------------------------------------------------------------------------
SELECT TOP 1000 *
FROM
    CRM_ERP_DWH.bronze.erp_cust_az12;

----------------------------------------------------------------------------------------------------
-- Ad-hoc Analysis: Quick look at the top 1000 records in the silver 'crm_cust_info' table.
----------------------------------------------------------------------------------------------------
SELECT TOP 1000 *
FROM
    CRM_ERP_DWH.silver.crm_cust_info;

-- Analyzing cid
SELECT cid
      ,bdate
      ,gen
  FROM CRM_ERP_DWH.bronze.erp_cust_az12
WHERE cid LIKE '%AW00011000%'

SELECT * 
FROM CRM_ERP_DWH.silver.crm_cust_info
WHERE cst_key = 'AW00011000'

-- Data Standardization
-- Data Fix for cid column
-- Keeping cid column consistent by removing NAS from some of the columns
SELECT 
	CASE 
		WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
		ELSE cid
	END AS cid
      ,bdate
      ,gen
FROM CRM_ERP_DWH.bronze.erp_cust_az12

-- Ensuring NAS is removed
-- Expectation : No results
SELECT 
	CASE 
		WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
		ELSE cid
	END AS cid
      ,bdate
      ,gen
FROM CRM_ERP_DWH.bronze.erp_cust_az12
WHERE CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) ELSE cid END 
	NOT IN (SELECT DISTINCT cst_key FROM CRM_ERP_DWH.silver.crm_cust_info)
	;

-- Dates Validation
-- Identifying Out of Range Dates
SELECT DISTINCT bdate
FROM CRM_ERP_DWH.bronze.erp_cust_az12
WHERE bdate < '1924-01-01' 
OR bdate > GETDATE();

SELECT MIN(bdate) AS min_bdate, MAX(bdate) AS max_bdate
FROM CRM_ERP_DWH.bronze.erp_cust_az12

-- Handling future bdate
SELECT 
	CASE 
		WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
		ELSE cid
	END AS cid,
    CASE WHEN bdate > GETDATE() THEN NULL ELSE bdate END AS bdate,
    gen
FROM CRM_ERP_DWH.bronze.erp_cust_az12


-- Data Standardization & Consistency
SELECT DISTINCT gen
FROM CRM_ERP_DWH.bronze.erp_cust_az12

-- Fixing gender data as Male, Female, NULL
SELECT DISTINCT gen,
	CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
		 WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
		 ELSE 'n/a'
	END AS gen
FROM CRM_ERP_DWH.bronze.erp_cust_az12


SELECT
    CASE
        WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
        ELSE cid
    END AS cid,
    bdate,
    CASE
        WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
        WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
        ELSE 'n/a'
    END AS gender
FROM
    CRM_ERP_DWH.bronze.erp_cust_az12;


--=============================================================
-- ERP Location Info (erp_loc_a101)
--=============================================================

----------------------------------------------------------------------------------------------------
-- Object Name       : erp_loc_a101 (Bronze Layer)
-- Description       : Contains location information for ERP customers.
-- Use Case          : This table will be joined with the 'crm_cust_info' table
--                     in the CRM system to link customer details with their location.
-- Join Condition    : crm_cust_info.cst_key = erp_loc_a101.cid
----------------------------------------------------------------------------------------------------

----------------------------------------------------------------------------------------------------
-- Ad-hoc Analysis: Quick look at the top 1000 records in the bronze 'erp_loc_a101' table.
----------------------------------------------------------------------------------------------------
SELECT TOP 1000 *
FROM
    CRM_ERP_DWH.bronze.erp_loc_a101;

----------------------------------------------------------------------------------------------------
-- Ad-hoc Analysis: Quick look at the top 1000 records in the silver 'crm_cust_info' table.
----------------------------------------------------------------------------------------------------
SELECT TOP 1000 *
FROM
    CRM_ERP_DWH.silver.crm_cust_info;

-- Data Quality
-- Fixing cid of bronze.erp_loc_a101 so it can be joined with cst_key of silver.crm_cust_info
SELECT cid, REPLACE(cid, '-', '') as new_cid
FROM CRM_ERP_DWH.bronze.erp_loc_a101;

-- Data Standardization & Consistency
SELECT DISTINCT cntry
FROM CRM_ERP_DWH.bronze.erp_loc_a101
ORDER BY 1

-- Keeping Country name consistent and Handling Null and Blanks
SELECT 
	REPLACE(cid, '-', '') as cid,
	CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		 WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
		 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
		 ELSE cntry
	END AS cntry
FROM CRM_ERP_DWH.bronze.erp_loc_a101;

-- Objective : Ensuring country name is consitent for all records
-- Expected : No multiple variatio of same country and there is NO NULL or Blanks

SELECT DISTINCT cntry
FROM ( 
	SELECT 
		REPLACE(cid, '-', '') as cid,
		CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
			 WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
			 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
			 ELSE cntry
		END AS cntry
	FROM CRM_ERP_DWH.bronze.erp_loc_a101
) AS TEMP
ORDER BY 1;

-- OR
SELECT 
	DISTINCT cntry,
	CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		 WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
		 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
		 ELSE cntry
	END AS cntry
FROM CRM_ERP_DWH.bronze.erp_loc_a101


--=============================================================
-- ERP Product Category Info (erp_px_cat_g1v2)
--=============================================================

----------------------------------------------------------------------------------------------------
-- Object Name       : erp_px_cat_g1v2 (Bronze Layer)
-- Description       : Contains porudct category, sub category, and maintenance information for ERP products.
-- Use Case          : This table will be joined with the 'crm_prd_info' table
--                     in the CRM system to link products details with their category.
-- Join Condition    : crm_prd_info.cat_id = erp_px_cat_g1v2.id
----------------------------------------------------------------------------------------------------

----------------------------------------------------------------------------------------------------
-- Ad-hoc Analysis: Quick look at the top 1000 records in the bronze 'erp_px_cat_g1v2' table.
----------------------------------------------------------------------------------------------------
SELECT TOP 1000 *
FROM
    CRM_ERP_DWH.bronze.erp_px_cat_g1v2
ORDER BY 1;

----------------------------------------------------------------------------------------------------
-- Ad-hoc Analysis: Quick look at the top 1000 records in the silver 'crm_prd_info' table.
----------------------------------------------------------------------------------------------------
SELECT TOP 1000 *
FROM
    CRM_ERP_DWH.silver.crm_prd_info
ORDER BY 2;


-- Check for unwanted spaces for STRING Columns
SELECT * FROM CRM_ERP_DWH.bronze.erp_px_cat_g1v2
WHERE cat <> TRIM(cat)
OR subcat <> TRIM(subcat)
OR maintenance <> TRIM(maintenance)


-- Check for Data Standardization & Consistency
SELECT DISTINCT cat
FROM CRM_ERP_DWH.bronze.erp_px_cat_g1v2

SELECT DISTINCT subcat
FROM CRM_ERP_DWH.bronze.erp_px_cat_g1v2

SELECT DISTINCT maintenance
FROM CRM_ERP_DWH.bronze.erp_px_cat_g1v2
