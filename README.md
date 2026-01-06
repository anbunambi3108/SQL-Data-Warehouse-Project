# 🏗️ SQL Data Warehouse Project

**End-to-End Data Warehousing and Analytics in SQL Server**

A practical SQL Server data warehouse built from raw ERP and CRM CSV extracts, designed to produce a clean, analysis-ready star schema for reporting and SQL-based analytics.

## 📌 Overview

This project demonstrates an end-to-end data warehousing workflow using SQL Server. Raw ERP and CRM CSV extracts are ingested, cleaned, and integrated into a unified analytical model using a three-layer medallion architecture. The final Gold layer is modeled as a star schema to support consistent reporting, ad-hoc analysis, and business insights.

## ✨ Key Highlights

* Implements a Bronze, Silver, Gold medallion architecture in SQL Server
* Cleans and standardizes raw ERP and CRM data for analytical use
* Integrates multiple source systems into a single reporting model
* Designs a star schema optimized for SQL-based analytics
* Handles common data quality issues prior to analysis
* Produces clear documentation for business and analytics users

## ⚙️ Features

| Component           | Description                                        | Status     |
| ------------------- | -------------------------------------------------- | ---------- |
| Data Ingestion      | Load ERP and CRM CSV extracts into SQL Server      | ✅ Complete |
| Bronze Layer        | Raw storage with minimal transformation            | ✅ Complete |
| Silver Layer        | Data cleansing, normalization, and standardization | ✅ Complete |
| Gold Layer          | Star schema with fact and dimension tables         | ✅ Complete |
| Data Quality Checks | Handling missing, invalid, and inconsistent values | ✅ Complete |
| Analytics Support   | SQL-ready model for reporting and insights         | ✅ Complete |

## 🔬 Methodology

### 1. Data Ingestion

* Loaded ERP and CRM CSV files into SQL Server
* Preserved raw structure in the Bronze layer
* Applied minimal transformations to retain source fidelity

### 2. Data Refinement

* Cleaned missing, invalid, and inconsistent values
* Standardized data types, formats, and naming conventions
* Normalized entities across ERP and CRM sources in the Silver layer

### 3. Data Modeling

* Designed a star schema in the Gold layer
* Built fact tables for sales-related events
* Created dimension tables for customers, products, and time

### 4. Validation and Readiness

* Verified referential integrity between facts and dimensions
* Ensured consistency across integrated sources
* Confirmed the model supports analytical queries without rework

## 📊 Key Results

### Analytical Outcomes

* Reliable customer-level and product-level sales analysis
* Consistent metrics across ERP and CRM sources
* Simplified SQL queries due to dimensional modeling

### Business Insights Enabled

* Clear visibility into customer behavior patterns
* Identification of top-performing products
* Analysis of overall sales trends using a unified dataset

## 🧰 Technologies

### Tools

| Technology | Purpose                               |
| ---------- | ------------------------------------- |
| SQL Server | Data warehousing and analytics engine |
| T-SQL      | Data transformation and modeling      |
| CSV        | Source data format                    |
| SSMS       | Development and query execution       |

### Libraries / Features Used

```sql
-- Core SQL Server features
CREATE TABLE
INSERT INTO
CTEs
JOINS
WINDOW FUNCTIONS
STAR SCHEMA DESIGN
```
## 🧠 Skills Demonstrated

* Data warehousing architecture design
* SQL-based ETL development
* Data cleansing and standardization
* Dimensional modeling and star schema design
* Multi-source data integration
* Analytical query enablement

## 🚀 Getting Started

### Prerequisites

* SQL Server (Local or Express)
* SQL Server Management Studio (SSMS)

### Usage

1. Clone the repository
2. Load the provided CSV files into SQL Server
3. Execute Bronze layer ingestion scripts
4. Run Silver layer transformation scripts
5. Build Gold layer fact and dimension tables
6. Run analytical SQL queries on the Gold layer

Inspired by Data With Baraa.
