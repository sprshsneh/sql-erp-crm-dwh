/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'CRM_ERP_DWH' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas 
    within the database: 'bronze', 'silver', and 'gold'.
	
WARNING:
    Running this script will drop the entire 'CRM_ERP_DWH' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/

-- Switch to 'master' database
USE master;
GO

-- Check if the database exists and drop it
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'CRM_ERP_DWH')
BEGIN 
    ALTER DATABASE CRM_ERP_DWH SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE CRM_ERP_DWH;
END;
GO

-- Create the database
CREATE DATABASE CRM_ERP_DWH;
GO

-- Switch to 'CRM_ERP_DWH' database
USE CRM_ERP_DWH;
GO

-- Create schemas for different data layers
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
