This project demonstrates an end-to-end data engineering pipeline built using Databricks and PySpark.
The pipeline ingests trending video data from the YouTube Data API, performs data validation and transformation, stores the processed data in Delta Lake, and enables analytics using Spark SQL.

# ***Architecture***

YouTube API
    ↓
Databricks Notebook (Python)
    ↓
PySpark Transformations
    ↓
Delta Table
    ↓
Spark SQL Analytics

## Tools & Technologies

Databricks Community Edition

Python

PySpark

Spark SQL

Delta Lake

YouTube Data API

## Pipeline Steps

Pipeline Steps

Extract

Fetch trending video data using YouTube API.

Handle API authentication and query parameters.

Transform

Validate records (handle missing snippet or statistics).

Load

Store cleaned data into a Delta table for analytics.

Analyze

Run Spark SQL queries to identify:

Top viewed videos.


## Schedule pipeline using Databricks Jobs
And set this into Databricks JOB option to Run this Pipeline Every Week.

### Notes
API keys are excluded for security reasons.
