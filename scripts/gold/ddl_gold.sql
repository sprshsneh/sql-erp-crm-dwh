/*
===============================================================================
DDL Script: Create Gold Views (Star Schema)
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse.
    The Gold layer represents the final dimension and fact tables (**Star Schema**).

    Each view performs transformations and combines data from the Silver layer
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

USE CRM_ERP_DWH;
GO

-------------------------------------------------------------------------------
-- 1. Create Dimension: gold.dim_customers
-------------------------------------------------------------------------------

IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers
AS
SELECT
    -- Surrogate Key
    ROW_NUMBER() OVER (ORDER BY ci.cst_id) AS customer_key
    -- Business Keys
    ,ci.cst_id          AS customer_id
    ,ci.cst_key         AS customer_number
    -- Attributes
    ,ci.cst_firstname   AS first_name
    ,ci.cst_lastname    AS last_name
    ,la.cntry           AS country
    ,ci.cst_marital_status AS marital_status
    -- Data Cleansing/Enrichment Logic
    ,CASE
        WHEN ci.cst_gndr <> 'n/a'
            THEN ci.cst_gndr             -- CRM is the master table for gender info
        ELSE COALESCE(ca.gen, 'n/a')     -- Fallback to ERP data if CRM is 'n/a' or NULL
    END AS gender
    ,ca.bdate           AS birthdate
    ,ci.cst_create_date AS create_date
FROM
    CRM_ERP_DWH.silver.crm_cust_info AS ci
LEFT OUTER JOIN
    CRM_ERP_DWH.silver.erp_cust_az12 AS ca
    ON ci.cst_key = ca.cid
LEFT OUTER JOIN
    CRM_ERP_DWH.silver.erp_loc_a101 AS la
    ON ci.cst_key = la.cid;
GO

-------------------------------------------------------------------------------
-- 2. Create Dimension: gold.dim_products
-------------------------------------------------------------------------------
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO

CREATE VIEW gold.dim_products
AS
SELECT
    -- Surrogate Key (Ensuring a stable key based on hierarchy)
    ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key
    -- Business Keys
    ,pn.prd_id              AS product_id
    ,pn.prd_key             AS product_number
    -- Attributes
    ,pn.prd_nm              AS product_name
    ,pn.cat_id              AS category_id
    ,pc.cat                 AS category
    ,pc.subcat              AS subcategory
    ,pc.maintenance         AS maintenance
    ,pn.prd_cost            AS cost
    ,pn.prd_line            AS product_line
    ,pn.prd_start_dt        AS start_date
FROM
    CRM_ERP_DWH.silver.crm_prd_info AS pn
LEFT JOIN
    CRM_ERP_DWH.silver.erp_px_cat_g1v2 AS pc
    ON pc.id = pn.cat_id
WHERE
    pn.prd_end_dt IS NULL; -- Filter for current/active products
GO


-------------------------------------------------------------------------------
-- 3. Create Fact Table: gold.fact_sales
-------------------------------------------------------------------------------
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales
AS
SELECT
    -------------------
    -- Dimension Keys
    -------------------
    sd.sls_ord_num      AS order_number
    ,pr.product_key     AS product_key      -- Dimension Key from gold.dim_products
    ,cu.customer_key    AS customer_key     -- Dimension Key from gold.dim_customers
    -------------------
    -- Dates
    -------------------
    ,sd.sls_order_dt    AS order_dt
    ,sd.sls_ship_dt     AS shipping_date
    ,sd.sls_due_dt      AS due_dt
    -------------------
    -- Measures
    -------------------
    ,sd.sls_sales       AS sales_amount
    ,sd.sls_quantity    AS quantity
    ,sd.sls_price       AS price
FROM
    CRM_ERP_DWH.silver.crm_sales_details AS sd
LEFT OUTER JOIN
    CRM_ERP_DWH.gold.dim_products AS pr
    ON sd.sls_prd_key = pr.product_number
LEFT OUTER JOIN
    CRM_ERP_DWH.gold.dim_customers AS cu
    ON sd.sls_cust_id = cu.customer_id;
GO
