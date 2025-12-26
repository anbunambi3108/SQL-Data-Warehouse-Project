/*
=============================================================
Create Database and Schemas
=============================================================
Purpose
    Creates the DataWarehouse database and initializes three schemas: bronze, silver, and gold.

Behavior
    - Checks whether the DataWarehouse database exists.
    - If it exists, drops it and recreates it.
    - Creates the bronze, silver, and gold schemas in the recreated database.

Warning
    This script is destructive. If DataWarehouse already exists, it will be dropped.
    All objects and data in that database will be permanently removed.
    Run only when this is intended, and confirm backups are available if needed.
*/

USE master;
GO /* GO: separate batches when working with multiple SQL statements. */

-- Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;
GO

-- Create the 'DataWarehouse' database
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

-- Create Schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
