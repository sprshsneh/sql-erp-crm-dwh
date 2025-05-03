/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

-- ====================================================================
-- Checking 'silver.crm_cust_info'
-- ====================================================================
-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results
SELECT cst_id,
       COUNT(*) AS tot_cnt
FROM CRM_ERP_DWH.silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Check for unwanted spaces
-- Expectation : No Results
SELECT cst_firstname
FROM CRM_ERP_DWH.silver.crm_cust_info
WHERE cst_firstname <> TRIM(cst_firstname);

SELECT cst_lastname
FROM CRM_ERP_DWH.silver.crm_cust_info
WHERE cst_lastname <> TRIM(cst_lastname);

SELECT cst_gndr
FROM CRM_ERP_DWH.silver.crm_cust_info
WHERE cst_gndr <> TRIM(cst_gndr);

-- Count of unwanted spaces in each column
SELECT 'cst_firstname' AS column_name, COUNT(*) AS count_of_unwanted_spaces
FROM CRM_ERP_DWH.silver.crm_cust_info
WHERE cst_firstname <> TRIM(cst_firstname)
UNION ALL
SELECT 'cst_lastname' AS column_name, COUNT(*) AS count_of_unwanted_spaces
FROM CRM_ERP_DWH.silver.crm_cust_info
WHERE cst_lastname <> TRIM(cst_lastname)
UNION ALL
SELECT 'cst_gndr' AS column_name, COUNT(*) AS count_of_unwanted_spaces
FROM CRM_ERP_DWH.silver.crm_cust_info
WHERE cst_gndr <> TRIM(cst_gndr)
UNION ALL
SELECT 'cst_marital_status' AS column_name, COUNT(*) AS count_of_unwanted_spaces
FROM CRM_ERP_DWH.silver.crm_cust_info
WHERE cst_marital_status <> TRIM(cst_marital_status);

-- Data Standardization & Consistency
SELECT DISTINCT cst_gndr
FROM CRM_ERP_DWH.silver.crm_cust_info;


-- ====================================================================
-- Checking 'silver.crm_prd_info'
-- ====================================================================


------------------------------------------------------------
-- Integerity Check
------------------------------------------------------------

-- Check for Nulls or Duplicates in primary key
-- Expectation : No Results
SELECT prd_id,
       COUNT(1) AS tot_cnt
FROM CRM_ERP_DWH.silver.[crm_prd_info]
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

------------------------------------------------------------
-- Quality Check
------------------------------------------------------------

-- Check for unwanted spaces
-- Expectation : No Results
SELECT prd_nm
FROM CRM_ERP_DWH.silver.[crm_prd_info]
WHERE prd_nm <> TRIM(prd_nm);

-- Check for Nulls or Negative Numbers
-- Expectation : No Results
SELECT prd_cost
FROM CRM_ERP_DWH.silver.[crm_prd_info]
WHERE prd_cost < 0 OR prd_cost IS NULL;

------------------------------------------------------------
-- Cardinality Check
------------------------------------------------------------

-- Data Standardization & Consistency
SELECT DISTINCT prd_line
FROM CRM_ERP_DWH.silver.crm_prd_info;

-- Check for Invalid Date Orders
SELECT *
FROM CRM_ERP_DWH.silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

-- Final Look
SELECT *
FROM CRM_ERP_DWH.silver.[crm_prd_info];


-- ====================================================================
-- Checking 'silver.crm_sales_details'
-- ====================================================================

SELECT sls_ord_num
      ,sls_prd_key
      ,sls_cust_id
      ,sls_order_dt
      ,sls_ship_dt
      ,sls_due_dt
      ,sls_sales
      ,sls_quantity
      ,sls_price
  FROM CRM_ERP_DWH.silver.crm_sales_details;

------------------------------------------------------------
-- Integerity Check
------------------------------------------------------------

-- Checking PK and FK relation between crm_prd_info and crm_sales_details
-- Expectation : No Results
SELECT * 
FROM CRM_ERP_DWH.silver.crm_sales_details 
WHERE sls_prd_key NOT IN (SELECT prd_key FROM CRM_ERP_DWH.silver.crm_prd_info)

-- Checking PK and FK relation between crm_prd_info and crm_cust_info
-- Expectation : No Results
SELECT * 
FROM CRM_ERP_DWH.silver.crm_sales_details 
WHERE sls_cust_id NOT IN (SELECT cst_id FROM CRM_ERP_DWH.silver.crm_cust_info)

------------------------------------------------------------
-- Quality Check
------------------------------------------------------------

-- Check for unwanted spaces
-- Expectation : No Results
SELECT sls_ord_num
FROM CRM_ERP_DWH.silver.crm_sales_details
WHERE sls_ord_num <> TRIM(sls_ord_num);

-- Check for Invalid Date

-- Ideal Order : Order Date < Ship Date OR Due Date
-- Expectation : No Results
SELECT *
FROM CRM_ERP_DWH.silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt 
OR sls_order_dt > sls_due_dt

-- Check for Business Rules
-- Sales = Quantity * Price
-- Sales, Quantity, Price can't be Negative, Zeros, or Nulls
-- Expectation : No Results
SELECT
	sls_sales,
	sls_quantity,
	sls_price
FROM CRM_ERP_DWH.silver.crm_sales_details
WHERE sls_sales <> (sls_quantity * sls_price)
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price


-- Final Look
SELECT *
FROM CRM_ERP_DWH.silver.crm_sales_details;


-- ====================================================================
-- Checking 'silver.erp_cust_az12'
-- ====================================================================
SELECT *
FROM CRM_ERP_DWH.silver.erp_cust_az12;

-- Dates Validation
-- Objective : Identifying birth date greater than current date
-- Expectation : No results
SELECT DISTINCT bdate
FROM CRM_ERP_DWH.silver.erp_cust_az12
WHERE bdate > GETDATE() 
;

-- Data Standardization & Consistency Check
-- Expectatuon : Query should return only 'Male', 'Female', 'n/a'
SELECT DISTINCT gen
FROM CRM_ERP_DWH.silver.erp_cust_az12

--=============================================================
-- ERP Location Info (erp_loc_a101)
--=============================================================

SELECT DISTINCT cntry
FROM CRM_ERP_DWH.silver.erp_loc_a101
ORDER BY 1

-- Final Look
SELECT * 
FROM CRM_ERP_DWH.silver.erp_loc_a101

--=============================================================
-- ERP Product Info (erp_px_cat_g1v2)
--=============================================================

SELECT 'bronze' AS data_layer, 'erp_px_cat_g1v2' AS tbl_name, COUNT(1) AS tot_cnt FROM CRM_ERP_DWH.bronze.erp_px_cat_g1v2
UNION ALL
SELECT 'silver' AS data_layer, 'erp_px_cat_g1v2' AS tbl_name, COUNT(1) AS tot_cnt FROM CRM_ERP_DWH.silver.erp_px_cat_g1v2
