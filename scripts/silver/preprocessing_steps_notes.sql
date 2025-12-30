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
-- b) for missing values by default use 'n/a' or NULL or unknown and  (for this project using n/a)
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

-- 2. moving to the prd_key, it has a lot of informations so it is divided into 2 columns, 
-- the 1 to 5 of the prd_key is cat_id: SUBSTRING() is used to extract the specific part of a string
-- in the erp table the cat_id has '_' but in crm table we have '-': using REPLACE()
-- in the gold layer the cat_id in the erp table and the cat_id in the crm table will be joined

-- the rest of them is prd_key
-- the prd_key from crm_sales_details table and the prd_key from the crm_prd_info will be joined

SELECT 
prd_id,
prd_key,
REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
FROM bronze.crm_prd_info;
GO

-- 3. moving to the prd_nm
-- check if there are any unwanted space in the product names
SELECT 
prd_id,
prd_key,
REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
FROM bronze.crm_prd_info
WHERE prd_nm!=TRIM(prd_nm);
GO
-- output notes: there is none, so there are no unwanted spaces in the product names


-- 4. cheking the quality of prd_cost, if they have nulls or negative numbers
-- using ISNULL() to replace the NULL values with a specified replacement value, in this case it is 0.
SELECT 
prd_id,
prd_key,
REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
prd_nm,
ISNULL(prd_cost,0) AS prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
FROM bronze.crm_prd_info;
--WHERE prd_cost < 0 OR prd_cost IS NULL; 

-- output notes, there are only null values and no negatives, so replace null with 0's


-- 5. prd_line has abbrivations, expanding them to maintain and store meaningful values
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

-- 6.check for invalid date order : Start date and end date
-- end date must not be earlier than the start date
-- the end date of the first history should be smaller than the start of the next record 
-- each new record must have a start date

-- to resolve this: derive the end date from the start date of the NEXT record - 1 (the end date is smaller than the next start date). 

SELECT * 
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt

-- building the logic
-- using LEAD() window functions.
-- what the LEAD() does: Access values from the next row within a window 
SELECT * ,
LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS prd_end_dt_test
FROM bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R','AC-HE-HL-U509');
GO
-- applying the logic to the whole table
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
CAST (prd_start_dt AS DATE) AS prd_start_dt,
CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt
FROM bronze.crm_prd_info;
GO

-- table 3: crm_sales_details
SELECT *
FROM bronze.crm_sales_details;
GO

-- 1. checking for extra spaces in the sls_ord_num
SELECT sls_ord_num
FROM bronze.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num);
-- output notes: no issues

-- 2.checking for extra spaces in sls_prd_key
SELECT sls_prd_key
FROM bronze.crm_sales_details
WHERE sls_prd_key != TRIM(sls_prd_key);
-- output notes: no issues

-- checking if there are any issues while joining both the tables
SELECT sls_prd_key
FROM bronze.crm_sales_details
WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info);
-- output notes: no issues

--3. checking if there are any issues while joining both the tables
SELECT sls_cust_id
FROM bronze.crm_sales_details
WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info);
GO
-- output notes: no issues 

--4. for the dates, the data type for them is integer and not dates
-- need to change the data type from integer to date and clean up the format

-- check if they have any negative values or 0's
SELECT 
NULLIF(sls_order_dt,0) AS sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt < 0 OR sls_order_dt <= 0;
-- output notes: there are no negative values, but there are a lot of 0s

-- do deal with this issue, using the NULLIF() fuction - returns NULL if two given values are equal; otherwise it returns the first expression
-- In this case the length of the date must be 8. It is in the format, yyyymmdd
-- if the length is less than 8 or higher than 8 then there is issue
SELECT 
NULLIF(sls_order_dt,0) AS sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt < 0 OR 
	  sls_order_dt <= 0 OR 
	  LEN(sls_order_dt) !=8 ;

-- check if the date is within range, 
SELECT 
NULLIF(sls_order_dt,0) AS sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 OR 
	  LEN(sls_order_dt) !=8 OR
	  sls_order_dt > 20500101 OR
	  sls_order_dt < 19000101
-- no out of range date, looks good
-- doing the same for sls_ship_dt and sls_due_dt
-- since there were issues in the sls_order_dt for the safer side also do the same for sls_ship_dt and sls_due_dt

-- to solve the issues encontered so far:
SELECT 
sls_ord_num,
sls_prd_key,
sls_cust_id,
CASE WHEN sls_order_dt = 0 OR 
	  LEN(sls_order_dt) !=8 THEN NULL
	  ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
END AS sls_order_dt,
CASE WHEN sls_ship_dt = 0 OR 
	  LEN(sls_ship_dt) !=8 THEN NULL
	  ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
END AS sls_ship_dt,
CASE WHEN sls_due_dt = 0 OR 
	  LEN(sls_due_dt) !=8 THEN NULL
	  ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
END AS sls_due_dt,
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_details;

-- Also checking if the order date is ealier than the shipping and due date,
-- cause it does not make any sense to deliver an iten without the order itself
-- so first the product is ordered then only shipped
-- checking for invalid date orders
SELECT * FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR 
sls_order_dt > sls_due_dt;
-- output: there is no issues

-- 5.Moving to sales, quantity and price:
-- REMEMBER THE BUSINESS RULES:
-- Sales = Quantity * Price
-- All sales quantity and price information should  be Positive ; Not allowed : Negative, Zero or Null.

SELECT sls_sales,
	   sls_quantity, 
	   sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL
OR sls_quantity IS NULL
OR sls_price IS NULL
OR sls_sales <= 0
OR sls_quantity <= 0
OR sls_price <= 0
ORDER BY sls_sales,
	   sls_quantity, 
	   sls_price;

-- output notes: there are issues with the quality of sales and price data
-- there are negavtive values, 0's and Nulls 
-- there are worng calculations
-- such issues must be communicated with the experts ( business or source system) and discussed
-- #1. Data issues will be fixed directly in the source system
-- #2. Data issues has to be fixed in data warehouse
-- either of them but should first be discussed with the expert and get their guidance

-- Consider the following rules:
-- If Sales is negative, zero or null derive it using Quantity and price
-- If price is zero or null, calculate it using sales and quantity
-- if price is neagtive convert it to the positive value

SELECT 
	   CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales!= sls_quantity* ABS(sls_price)
	   THEN sls_quantity * ABS(sls_price)
	   ELSE sls_sales
	   END as sls_sales,
	   sls_quantity, 
	   CASE WHEN sls_price IS NULL OR sls_price <=0 
	   THEN sls_sales/NULLIF(sls_quantity,0)
	   ELSE sls_price
	   END as sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL
OR sls_quantity IS NULL
OR sls_price IS NULL
OR sls_sales <= 0
OR sls_quantity <= 0
OR sls_price <= 0
ORDER BY sls_sales,
	   sls_quantity, 
	   sls_price;

-- Final query for table 3:
SELECT 
sls_ord_num,
sls_prd_key,
sls_cust_id,
CASE WHEN sls_order_dt = 0 OR 
	  LEN(sls_order_dt) !=8 THEN NULL
	  ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
END AS sls_order_dt,
CASE WHEN sls_ship_dt = 0 OR 
	  LEN(sls_ship_dt) !=8 THEN NULL
	  ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
END AS sls_ship_dt,
CASE WHEN sls_due_dt = 0 OR 
	  LEN(sls_due_dt) !=8 THEN NULL
	  ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
END AS sls_due_dt,
CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales!= sls_quantity* ABS(sls_price)
THEN sls_quantity * ABS(sls_price)
ELSE sls_sales
END as sls_sales,
sls_quantity,
CASE WHEN sls_price IS NULL OR sls_price <=0 
THEN sls_sales/NULLIF(sls_quantity,0)
ELSE sls_price
END as sls_price
FROM bronze.crm_sales_details;

-- table:4 erp_cust_az12
SELECT
cid,
bdate,
gen
FROM bronze.erp_cust_az12;

-- the cid in this table is joined with the cst_key in crm_cust_info table
-- check if both the tables can be joined
SELECT cid
FROM bronze.erp_cust_az12
--WHERE cid NOT IN (SELECT cst_key FROM silver.crm_cust_info);
WHERE cid LIKE '%AW00019784'

-- there are extra characters (NAS) in the cid that are not in the cst_key
-- without the extra characters the cid exist

SELECT
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4, LEN(cid))
ELSE cid
END cid,
bdate,
gen
FROM bronze.erp_cust_az12;
--WHERE CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4, LEN(cid))
--ELSE cid
--END NOT IN (SELECT cst_key FROM silver.crm_cust_info);

-- no issue when joining both the tables

-- checking if there are any old bdates or birthdates in the future
SELECT bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE();

-- output notes: there are some invalid birthdates some of then unrealistically old and some of them in the future.

-- the future birthdate is a 100% big issue , which could be resolved by using NULL or 0
SELECT
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4, LEN(cid))
ELSE cid
END cid,
CASE WHEN bdate > GETDATE() THEN NULL
ELSE bdate
END AS bdate,
gen
FROM bronze.erp_cust_az12;

-- checking for data consistency and standardization
SELECT DISTINCT gen
FROM bronze.erp_cust_az12 

-- output notes: The data is inconsitent, there has to be only three values, Male, Female and n/a
SELECT
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4, LEN(cid))
ELSE cid
END cid,
CASE WHEN bdate > GETDATE() THEN NULL
ELSE bdate
END AS bdate,
--DISTINCT gen,
CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
	 WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
	 ELSE 'n/a'
	 END AS gen
FROM bronze.erp_cust_az12;


-- table 5: erp_loc_a101 Location of the customers
SELECT * FROM bronze.erp_loc_a101

-- the cid in erp_loc_a101 is connected with the cst_key in crm_cust_info
-- check if they can be connected without any issue

SELECT cid
FROM bronze.erp_loc_a101
WHERE cid NOT IN (SELECT cst_key FROM silver.crm_cust_info);

-- output notes: there is a - in the cid 

SELECT 
REPLACE(cid,'-','') as cid
FROM bronze.erp_loc_a101
WHERE REPLACE(cid,'-','') NOT IN (SELECT cst_key FROM silver.crm_cust_info);

-- checking for data consistency and standardization

SELECT DISTINCT cntry
FROM bronze.erp_loc_a101;

-- output notes: There are inconsitency
SELECT 
CASE WHEN TRIM(cntry) IN ('USA','US') THEN 'United States'
	 WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
	 ELSE TRIM(cntry)
	 END
	 AS cntry
	 FROM bronze.erp_loc_a101;

-- Final query
SELECT 
REPLACE(cid,'-','') as cid,
CASE WHEN TRIM(cntry) IN ('USA','US') THEN 'United States'
	 WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
	 ELSE TRIM(cntry)
	 END
	 AS cntry
FROM bronze.erp_loc_a101;

-- table 6: erp_px_cat_g1v2
SELECT * FROM bronze.erp_px_cat_g1v2;

-- Check for unwanted spaces
SELECT * FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM (subcat) OR maintenance != TRIM(maintenance)

-- Check for data consistency and standardisation
SELECT DISTINCT cat
FROM bronze.erp_px_cat_g1v2

SELECT DISTINCT subcat
FROM bronze.erp_px_cat_g1v2

SELECT DISTINCT maintenance
FROM bronze.erp_px_cat_g1v2

-- everything looks perfect in this table
