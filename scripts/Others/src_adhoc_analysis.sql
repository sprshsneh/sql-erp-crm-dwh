SELECT * FROM CRM_ERP_DWH.bronze.crm_cust_info

SELECT * FROM CRM_ERP_DWH.silver.etl_audit_log;


SELECT 'crm_cust_info' AS tbl_type, COUNT(1) AS cnt FROM CRM_ERP_DWH.silver.crm_cust_info UNION ALL
SELECT 'crm_prd_info' AS tbl_type, COUNT(1) AS cnt FROM CRM_ERP_DWH.silver.crm_prd_info UNION ALL
SELECT 'crm_sales_details' AS tbl_type, COUNT(1) AS cnt FROM CRM_ERP_DWH.silver.crm_sales_details UNION ALL
SELECT 'erp_cust_az12' AS tbl_type, COUNT(1) AS cnt FROM CRM_ERP_DWH.silver.erp_cust_az12 UNION ALL
SELECT 'erp_loc_a101' AS tbl_type, COUNT(1) AS cnt FROM CRM_ERP_DWH.silver.erp_loc_a101 UNION ALL
SELECT 'erp_px_cat_g1v2' AS tbl_type, COUNT(1) AS cnt FROM CRM_ERP_DWH.silver.erp_px_cat_g1v2
;

SELECT GETDATE();

EXEC silver.load_silver

SELECT COUNT(*) FROM CRM_ERP_DWH.silver.erp_cust_az12
DELETE [silver].[etl_audit_log]

SELECT * FROM silver.etl_audit_log