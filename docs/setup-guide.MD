# Setup Guide

## Prerequisites
- AWS account
- Snowflake account
- S3 bucket
- AWS Glue access
- IAM role for Glue
- IAM role/trust setup for Snowflake S3 integration

## Step 1: Create S3 bucket structure
Create the following structure:

s3://<YOUR_BUCKET>/covid/
- bronze/nytimes/
- bronze/covid_tracking/
- bronze/static/
- silver/cases_standardized/
- silver/testing_standardized/
- gold/dim_date/
- gold/dim_state/
- gold/fact_cases_state_daily/
- gold/fact_testing_state_daily/
- scripts/

## Step 2: Copy raw data into Bronze
Required source files:
- us_states.csv
- states_daily.csv
- states_abv.csv

## Step 3: Upload Glue scripts
Upload:
- `covid_silver_glue_job.py`
- `covid_gold_glue_job.py`

## Step 4: Run Silver Glue job
Input:
- Bronze S3 paths

Output:
- standardized Parquet files in Silver

## Step 5: Run Gold Glue job
Input:
- Silver S3 paths

Output:
- dimensional/fact Parquet files in Gold

## Step 6: Configure Snowflake
Run the SQL file in `sql/snowflake_setup_and_load.sql` to:
- create warehouse
- create database/schema
- create storage integration
- create stage
- create file format
- create tables
- load data from Gold
- validate tables

## Step 7: Run analytics queries
Use the final validation and reporting queries in Snowflake.
