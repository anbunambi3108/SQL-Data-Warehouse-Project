# Data Catalog for Gold Layer

## Overview

The Gold layer is the curated analytical representation of the warehouse. It exposes a star schema composed of dimension and fact structures intended for reporting and SQL-based analysis.

---

## 1. gold.dim_customers

**Purpose**
Customer reference data, enriched with basic demographic and geographic attributes.

**Columns**

| Column Name     | Data Type    | Description                                                                      |
| --------------- | ------------ | -------------------------------------------------------------------------------- |
| customer_key    | INT          | Surrogate key for the customer dimension record.                                 |
| customer_id     | INT          | Source-system customer identifier.                                               |
| customer_number | NVARCHAR(50) | Business-facing customer code used for tracking and lookup.                      |
| first_name      | NVARCHAR(50) | Customer first name.                                                             |
| last_name       | NVARCHAR(50) | Customer last name.                                                              |
| country         | NVARCHAR(50) | Customer country of residence.                                                   |
| marital_status  | NVARCHAR(50) | Customer marital status.                                                         |
| gender          | NVARCHAR(50) | Customer gender value as stored in the source (including non-applicable values). |
| birthdate       | DATE         | Customer date of birth (YYYY-MM-DD).                                             |
| create_date     | DATE         | Record creation date in the source system.                                       |

---

## 2. gold.dim_products

**Purpose**
Product reference data, including classification and operational attributes.

**Columns**

| Column Name          | Data Type    | Description                                                |
| -------------------- | ------------ | ---------------------------------------------------------- |
| product_key          | INT          | Surrogate key for the product dimension record.            |
| product_id           | INT          | Source-system product identifier.                          |
| product_number       | NVARCHAR(50) | Business-facing product code used for tracking and lookup. |
| product_name         | NVARCHAR(50) | Product name or description.                               |
| category_id          | NVARCHAR(50) | Source category identifier.                                |
| category             | NVARCHAR(50) | Product category.                                          |
| subcategory          | NVARCHAR(50) | Product subcategory.                                       |
| maintenance_required | NVARCHAR(50) | Indicates whether the product requires maintenance.        |
| cost                 | INT          | Product cost value as provided by the source.              |
| product_line         | NVARCHAR(50) | Product line or series classification.                     |
| start_date           | DATE         | Product availability start date.                           |

---

## 3. gold.fact_sales

**Purpose**
Transactional sales facts at the order line level, keyed to customer and product dimensions.

**Columns**

| Column Name   | Data Type    | Description                                           |
| ------------- | ------------ | ----------------------------------------------------- |
| order_number  | NVARCHAR(50) | Sales order identifier.                               |
| product_key   | INT          | Foreign key to gold.dim_products.                     |
| customer_key  | INT          | Foreign key to gold.dim_customers.                    |
| order_date    | DATE         | Date the order was placed.                            |
| shipping_date | DATE         | Date the order was shipped.                           |
| due_date      | DATE         | Payment due date.                                     |
| sales_amount  | INT          | Line-level sales amount in whole currency units.      |
| quantity      | INT          | Units sold for the line item.                         |
| price         | INT          | Unit price for the line item in whole currency units. |

