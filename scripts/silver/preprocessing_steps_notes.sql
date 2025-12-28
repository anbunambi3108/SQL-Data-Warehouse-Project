---- clean and standardisation
--notes:
--Metadata Columns:
--- extra columns added by the data enginneers that do not originate from the source data.
--- we can add:
-- 1. create_date: the record's load timestamp. (when the record was loaded)
-- 2. update_date: the record's last update timestamp. (when the record got updated)
-- 3. source_system: the origin sysytem of the record. (to understand the origin of the given data)
-- 4. file_loaction: the file source of the record. (to understand the linkage from which file the data comes from)
--these are saviours if you have data issue in the data wearhouse, cause this will help to track exactly 
--where this issue happens and when also it is great to understand if we have gap in the data espesically when linking multiple data.
--It is like adding lables to everything and it will be useful when you have issue with the data

--step 1: Clean the tables in the bronze layer and load to the silver layer

--firstly, check the qulity of the data in the bronze layer, then write the transformation script to impove the qulity issue.
--Explore the tables in the bronze layer, clean up the data then load it to the silver layer

-- table 1: crm_cust_info

 SELECT * 
 FROM bronze.crm_cust_info;
 GO
 -- 1. check if the primary key (cst_id) is unique and not null
 -- goal: 
 --- to check if the primary key had duplicated (>1) 
 --- to check if they have any NULL values
SELECT cst_id, COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) >1 OR cst_id IS NULL;
GO
-- output notes: 5 of the ids have 2 or 3 duplicate values and 3 of them are NULL.
-- to investigate in depth filter by one id eg:29466
SELECT * FROM bronze.crm_cust_info
WHERE cst_id=29466;
GO
-- output notes: consider the timestamp or datevalue to help filter out the needed value. 
-- In this case we take the latest create date cause it hold the most recent and frest information

-- Preprocessing Step for cst_id
--- To do so rank the by latest create date and take only the lastest (falg_last = 1) create date
SELECT * 
FROM 
(SELECT *, ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
FROM bronze.crm_cust_info
WHERE cst_id IS NOT NULL) temp
WHERE flag_last =1

--2. Check if the string values (firsname, lastname, marital_status, gender) have unwanted spaces
SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname); -- TRIM() removes all the leading and trailing spaces from the string
-- if the original value is not equal to the same value after triming, it means there are spaces
-- output notes : 15 values has unwanted spaces
GO
SELECT cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);
-- output notes : 17 values has unwanted spaces
GO
SELECT cst_gndr
FROM bronze.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr);
-- output notes : 0 value has unwanted spaces
GO
SELECT cst_marital_status
FROM bronze.crm_cust_info
WHERE cst_marital_status != TRIM(cst_marital_status); 
-- output notes : 0 value has unwanted spaces

-- Preprocessing Step for cst_firstname and cst_lastname
SELECT
cst_id,
cst_key,
TRIM(cst_firstname),
TRIM(cst_lastname),
cst_marital_status,
cst_gndr,
cst_create_date
FROM(
SELECT *, ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
FROM bronze.crm_cust_info
WHERE cst_id IS NOT NULL
) temp
WHERE flag_last =1;
GO
-- 3. Check the consitency of values in low cardinality columns
-- Check for Data Standardization and Consistency
SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info;
GO

SELECT DISTINCT cst_marital_status
FROM bronze.crm_cust_info;
GO
-- a) in data wearhouse, the aim is to store clear and meaningful values rather than using abbriviated terms
-- b) for missing values by default use 'n/a' or NULL or unkonown and  (for this project using n/a)
-- follow (a & b) for the whole project.

-- Preprocessing Step for cst_gndr and cst_marital_status
SELECT
cst_id,
cst_key,
TRIM(cst_firstname),
TRIM(cst_lastname),
CASE WHEN UPPER(cst_marital_status) = 'S' THEN 'Single'
	 WHEN UPPER(cst_marital_status) = 'M' THEN 'Married'
	 ELSE 'n/a'
END cst_marital_status,
CASE WHEN UPPER(cst_gndr) = 'F' THEN 'Female'
	 WHEN UPPER(cst_gndr) = 'M' THEN 'Male'
	 ELSE 'n/a'
END cst_gndr,
cst_create_date
FROM(
SELECT *, ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
FROM bronze.crm_cust_info
WHERE cst_id IS NOT NULL
) temp
WHERE flag_last =1;
GO

-- to check the quality of the data in the silver layer, rerun the above queried from the bronze layer 
-- to verify thr quality of the data in the silver layer. 
-- Example:
	-- SELECT cst_id, COUNT(*)
	-- FROM silver.crm_cust_info
	-- GROUP BY cst_id
	-- HAVING COUNT(*) >1 OR cst_id IS NULL;
-- output notes: if nothing shows up then the quality of the data is perfect. 
 SELECT * 
 FROM silver.crm_cust_info;
 GO
-----------------------------------------------------------------------------------------------------------------------
-- table 2: crm_prd_info
SELECT * 
FROM bronze.crm_prd_info;
GO
-- 1. check if the primary key (cst_id) is unique and not null
-- goal: 
--- to check if the primary key had duplicated (>1) 
--- to check if they have any NULL values
SELECT prd_id, COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) >1 OR prd_id IS NULL;
GO
-- output notes: nothing available, looks good.

-- 2. 
SELECT 
prd_id,
prd_key,
REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
prd_nm,
ISNULL(prd_cost,0) AS prd_cost,
CASE WHEN UPPER(TRIM(prd_line))='M' THEN 'Mountain'
	 WHEN UPPER(TRIM(prd_line))='R' THEN 'Road'
	 WHEN UPPER(TRIM(prd_line))='S' THEN 'Other Sales'
	 WHEN UPPER(TRIM(prd_line))='T' THEN 'Touring'
	 ELSE 'n/a'
END prd_line,
prd_start_dt,
prd_end_dt
FROM bronze.crm_prd_info;
GO
