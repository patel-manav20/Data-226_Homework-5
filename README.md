**📘 Overview**

This Airflow DAG automates an ETL pipeline that extracts stock data from Alpha Vantage, transforms it, and loads it into Snowflake.

All tasks use the @task decorator with proper dependencies and configurations.

**🧱 DAG Summary**

DAG Name: DATA-226_Homework-5

**Tasks:**

extract_data → Fetches data from Alpha Vantage

transform_data → Cleans and prepares the dataset

load_data → Loads data into Snowflake with a full-refresh transaction

**Task Flow:**

**extract_data**  >>  **transform_data**  >>  **load_data**


