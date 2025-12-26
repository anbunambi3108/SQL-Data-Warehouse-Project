## SQL Data Warehouse Project

This project presents an end-to-end data warehousing and analytics workflow in SQL Server. It begins with raw ERP and CRM CSV extracts and produces an analysis-ready model for reporting and SQL-based analytics.

## Data Architecture

The warehouse follows a three-layer medallion structure:

* **Bronze (raw):** ERP and CRM CSV data is ingested into SQL Server with minimal transformation.
* **Silver (refined):** Data is cleaned, standardized, and normalized. Quality issues are addressed, and formats are aligned across sources.
* **Gold (modeled):** Data is modeled as a star schema with fact and dimension tables to support analytical queries.

## Project Scope and Requirements

### Objective

Consolidate sales-related data into a SQL Server data warehouse that supports consistent reporting and analysis.

### Specifications

* **Sources:** Two systems (ERP and CRM), provided as CSV files.
* **Data quality:** Handle missing, inconsistent, and invalid values prior to analytics.
* **Integration:** Combine both sources into a single analytical model.
* **Scope:** The project uses the latest dataset only; historical tracking is out of scope.
* **Documentation:** Provide clear documentation of the final model for business and analytics users.

## Analytics and Reporting

The Gold layer supports SQL-based analysis of:

* Customer behavior
* Product performance
* Sales trends

## Credits
Inspired by Data With Baraa.
