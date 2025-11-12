[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/_7XkBFlv)
﻿# Project Title: Sales Performance Optimization using Data Pipelines and BI
## 🎯 Project Goal
-  To construct a robust, scalable, and observable data pipeline using Prefect to move raw sales data into a MySQL database, and subsequently create a dynamic PowerBI dashboard for actionable business insights into sales drivers.

## ❓ The Business Problem
- The raw data contains car sales records, including vehicle specifications, regional sales figures, and a categorical 'Sales Classification' (High/Low). 
### The core challenge is:

- Which vehicle attributes (Model, Price, Region, Fuel Type) are the strongest predictors for achieving a "High" Sales Classification?

- We need an automated system to process this data and a visual report to answer this question and guide strategic business decisions related to inventory, pricing, and marketing.

## 🛠️ Tech Stack & Requirements
This project requires proficiency in the following tools:

## Tools Overview

| Tool      | Purpose                                                        |
|-----------|----------------------------------------------------------------|
| **Python** | Core language for ETL (Pandas, SQLAlchemy).                    |
| **Prefect** | Workflow orchestration and pipeline management.               |
| **MySQL**  | Central data warehouse for structured storage.                |
| **PowerBI** | Business Intelligence and interactive data visualization.    |
| **MS Excel** | Initial data exploration and sanitation planning.           |


## 🚀 Tasks for Submission
Students must submit the following components:

- etl_flow.py: A complete, runnable Python script defining the Prefect tasks (extract_data, transform_data, load_data) and the main flow (sales_etl_flow).

- requirements.txt: A file listing all necessary Python dependencies (e.g., prefect, pandas, sqlalchemy, mysql-connector-python).

- schema.sql: The SQL script used to create the car_sales table in MySQL.

- car_sales_data.csv: (The raw data file used for the pipeline).

- PowerBI report file

Note: Successful pipeline execution must demonstrate clean, standardized data loaded into the correct MySQL table.

---

## 📊 Power BI Dashboard

You can explore the interactive Power BI report here:  
👉 [View Power BI Dashboard](https://app.powerbi.com/view?r=eyJrIjoiZTEyOGQwMTctMjQ3NC00ZjczLWJiMmUtYzZhNzQ0YmUyMTg0IiwidCI6IjdhZmI5ZTIyLTkzMDgtNDE4Ni04ZTI5LWVhMjMxZmYzYmFmNyIsImMiOjN9)
