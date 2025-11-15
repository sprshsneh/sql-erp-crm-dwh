/*
===============================================================================
Silver Layer Analysis and Gold Layer Build: dim_customers
===============================================================================
*/


--Base Logic for View
SELECT
	ci.cst_id,
	ci.cst_key,
	ci.cst_firstname,
	ci.cst_lastname,
	ci.cst_marital_status,
	ci.cst_gndr,
	ci.cst_create_date,
	ca.bdate,
	ca.gen,
	la.cntry
FROM CRM_ERP_DWH.silver.crm_cust_info ci
LEFT OUTER JOIN CRM_ERP_DWH.silver.erp_cust_az12 ca ON ci.cst_key = ca.cid
LEFT OUTER JOIN CRM_ERP_DWH.silver.erp_loc_a101 la ON ci.cst_key = la.cid
  ;

-- Checking Duplicates
SELECT COUNT(1) as dup_cnts
FROM ( 
	SELECT
		ci.cst_id,
		ci.cst_key,
		ci.cst_firstname,
		ci.cst_lastname,
		ci.cst_marital_status,
		ci.cst_gndr,
		ci.cst_create_date,
		ca.bdate,
		ca.gen,
		la.cntry
	FROM CRM_ERP_DWH.silver.crm_cust_info ci
	LEFT OUTER JOIN CRM_ERP_DWH.silver.erp_cust_az12 ca ON ci.cst_key = ca.cid
	LEFT OUTER JOIN CRM_ERP_DWH.silver.erp_loc_a101 la ON ci.cst_key = la.cid
) t
GROUP BY cst_id
HAVING COUNT(1) > 1 ;


-- Analysing Gender COlumn 
SELECT DISTINCT
      ci.cst_gndr, 
	  ca.gen,
	  -- Handling inconsistent data for gender
	  CASE WHEN ci.cst_gndr <> 'n/a' THEN ci.cst_gndr -- CRM is the master for gender info
		ELSE COALESCE(ca.gen, 'n/a')
	  END AS new_gen
  FROM CRM_ERP_DWH.silver.crm_cust_info ci
  LEFT OUTER JOIN CRM_ERP_DWH.silver.erp_cust_az12 ca ON ci.cst_key = ca.cid
  LEFT OUTER JOIN CRM_ERP_DWH.silver.erp_loc_a101 la ON ci.cst_key = la.cid
  ORDER BY 1,2


-- Integerated Query 
SELECT
	ROW_NUMBER() OVER (order BY cst_id) AS customer_key, -- Surrogate Key
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	la.cntry AS country,
	ci.cst_marital_status AS marital_status,
	  CASE WHEN ci.cst_gndr <> 'n/a' THEN ci.cst_gndr -- CRM is the master table for gender info
		ELSE COALESCE(ca.gen, 'n/a')
	  END AS gender,
	ca.bdate AS birthdate,
	ci.cst_create_date AS create_date
FROM CRM_ERP_DWH.silver.crm_cust_info ci
LEFT OUTER JOIN CRM_ERP_DWH.silver.erp_cust_az12 ca ON ci.cst_key = ca.cid
LEFT OUTER JOIN CRM_ERP_DWH.silver.erp_loc_a101 la ON ci.cst_key = la.cid
  ;

-- Final View
CREATE VIEW gold.dim_customers AS 
	SELECT
		ROW_NUMBER() OVER (order BY cst_id) AS customer_key, -- Surrogate Key
		ci.cst_id AS customer_id,
		ci.cst_key AS customer_number,
		ci.cst_firstname AS first_name,
		ci.cst_lastname AS last_name,
		la.cntry AS country,
		ci.cst_marital_status AS marital_status,
		  CASE WHEN ci.cst_gndr <> 'n/a' THEN ci.cst_gndr -- CRM is the master table for gender info
			ELSE COALESCE(ca.gen, 'n/a')
		  END AS gender,
		ca.bdate AS birthdate,
		ci.cst_create_date AS create_date
	FROM CRM_ERP_DWH.silver.crm_cust_info ci
	LEFT OUTER JOIN CRM_ERP_DWH.silver.erp_cust_az12 ca ON ci.cst_key = ca.cid
	LEFT OUTER JOIN CRM_ERP_DWH.silver.erp_loc_a101 la ON ci.cst_key = la.cid
  ;


  -- Final View Load Check 
 SELECT * FROM CRM_ERP_DWH.gold.dim_customers;

/*
===============================================================================
Silver Layer Analysis and Gold Layer Build: dim_products
===============================================================================
*/

-- Product Information
-- Available at places - CRM and ERP

SELECT TOP 10 * FROM CRM_ERP_DWH.silver.crm_prd_info ;

SELECT *  FROM CRM_ERP_DWH.gold.dim_customers;

-- Base logic for view
SELECT 
		ROW_NUMBER() 
		OVER (ORDER BY pn.prd_start_dt, pn.prd_key)
								AS product_key  -- Surrogate Key
	  ,	prd_id					AS product_id
      , pn.prd_key				AS product_number
      , pn.prd_nm				AS product_name
	  , pn.cat_id				AS category_id
	  , pc.cat					AS category
	  , pc.subcat				AS subcategory
	  , pc.maintenance			AS maintenance
      , pn.prd_cost				AS cost
      , pn.prd_line				AS product_line
      , pn.prd_start_dt			AS start_date
  FROM CRM_ERP_DWH.silver.crm_prd_info pn
  LEFT JOIN CRM_ERP_DWH.silver.erp_px_cat_g1v2 pc
  ON pc.id = pn.cat_id
  WHERE prd_end_dt IS NULL -- to pull latest data and ignore histrorical data
  ;

WITH cte_base_data AS (
	SELECT prd_id
		  , pn.cat_id
		  , pn.prd_key
		  , pn.prd_nm
		  , pn.prd_cost
		  , pn.prd_line
		  , pn.prd_start_dt
		  , pc.cat
		  , pc.subcat
		  , pc.maintenance
	  FROM CRM_ERP_DWH.silver.crm_prd_info pn
	  LEFT JOIN CRM_ERP_DWH.silver.erp_px_cat_g1v2 pc
	  ON pc.id = pn.cat_id
	  WHERE prd_end_dt IS NULL
	  )
-- Main Query : Uniqueness check ( 0 Duplicates)
SELECT prd_key, COUNT(1) AS total_records
FROM cte_base_data
GROUP BY prd_key
HAVING COUNT(1) > 1;


-- Final Check
SELECT * FROM CRM_ERP_DWH.gold.dim_products;


/*
===============================================================================
Silver Layer Analysis and Gold Layer Build: fact_sales
===============================================================================
*/

-- Final view
SELECT sls_ord_num		AS order_number
      ,pr.product_key	AS product_key   -- Dimesnion Key from dimension table - product
      ,cu.customer_key	AS customer_key  -- Dimesnion Key from dimension table - customers
      ,sls_order_dt		AS order_dt
      ,sls_ship_dt		AS shipping_date
      ,sls_due_dt		AS due_dt
      ,sls_sales		AS sales_amount
      ,sls_quantity		AS quantity
      ,sls_price		AS price
  FROM CRM_ERP_DWH.silver.crm_sales_details sd
  LEFT OUTER JOIN CRM_ERP_DWH.gold.dim_products pr 
  ON sd.sls_prd_key = pr.product_number
  LEFT OUTER JOIN CRM_ERP_DWH.gold.dim_customers cu 
  ON sd.sls_cust_id = cu.customer_id ;


-- Final Check
SELECT * FROM CRM_ERP_DWH.gold.fact_sales;


-- Dimesnion Modelling Check 

-- Foreign Key Integerity (Dimensions)
-- Expected Records : 0 
SELECT *
FROM CRM_ERP_DWH.gold.fact_sales f
LEFT JOIN CRM_ERP_DWH.gold.dim_customers c
ON c.customer_key = f.customer_key
LEFT JOIN CRM_ERP_DWH.gold.dim_products p
ON c.customer_key = p.product_key
WHERE c.customer_key IS NULL;
