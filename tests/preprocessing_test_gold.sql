/*
===============================================================================
Gold Layer Quality Checks
===============================================================================
Script Purpose
    Validates key integrity and dimensional connectivity in the gold layer.

    This script performs three checks:
    1) Ensures customer_key is unique in gold.dim_customers.
    2) Ensures product_key is unique in gold.dim_products.
    3) Verifies that every row in gold.fact_sales has matching records in
       gold.dim_customers and gold.dim_products.

Usage
    Run this script after creating or refreshing the gold layer views/tables,
    and after any changes to the silver-to-gold transformation logic.

Interpretation
    - The duplicate key checks should return no rows. Any output indicates
      key collisions that must be investigated.
    - The connectivity check should return no rows. Any output indicates
      orphaned fact rows where the referenced customer or product does not
      exist in the corresponding dimension.
===============================================================================
*/
SELECT 
    customer_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;

-- Checking 'gold.product_key'
-- Check for Uniqueness of Product Key in gold.dim_products
-- Expectation: No results 
SELECT 
    product_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;
-- Checking 'gold.fact_sales'
-- Check the data model connectivity between fact and dimensions
SELECT * 
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
WHERE p.product_key IS NULL OR c.customer_key IS NULL  
