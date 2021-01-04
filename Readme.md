# Covid-19 Capstone Project

## Introduction
* what other topic than covid in 2020 could be covered
* a lot of data available

## Scope of the project
* gather covid data which reflects the dynamic of the pandemic in 2020 and the response of the people and the public towards the pandemic
* where did it start?
* which effects did the pandemic have on mobility?
* how was it covered in the media?
* which behavioural changes concerning internet search did the pandemic have?
* how did government policies impact the covid cases and vice versa?

## Data
* Google open data
    - Epidemioloy - Prio 1
    - Mobility - Prio 1
    - Search Trends - Prio 2

* GDELT project
    - covid extract
    - source: http://data.gdeltproject.org/events/index.html
    - doc: https://www.gdeltproject.org/data.html#documentation

* Oxford Gov Response
    - https://github.com/OxCGRT/covid-policy-tracker/blob/master/documentation/codebook.md


## Step 1 - Simple

1. Ingestion
    * basic: download (sample) data and use locally
    * advanced 0: download data and store to s3
    * advanced 1: call api from plain python
    * advanced 2: call api from pyspark
2. Use pandas to explore
    * advanced 1: use spark to explore data
3. Write to S3 data lake
4. Create redshift dwh


## Questions

* what were the first restrictions and where?
* what searchterm have the highest increases?



# Architecture

## Stages

1. RAW
* raw data as it came
* no deletes, updates
* very deep and adhoc analysis with Spark

2. Structured and Cleaned Data Lake
* structured, cleaned, reduced data
* leaning to questions that have interest
* parquet files, partioned and compressed
* purpose: analytical teams with tech knowledge, eda, ml & Co.

3. DataMart in Redshift
* purpose: deliver high performance queries for BI tools and dashboards
* reduced data, facts etc. according to questions that are frequently asked
* Schema at: dbdiagram.io

4. BI Tool
* Accessing either the data lake or the data mart
* in AWS ecosystem: quicksight
* or any other solution depending on the budget, business requirements etc

## Data pipeline

1. Ingestion
    * first approach
        * manual download
        * upload to S3
    * advanced approach
        * script with connecting APIs
        * API-Connection to the data sources

2. Creating data lake
    * Spark-submit on EMR
    * writing to parquet

3. Building data mart
    * etl accessing the data lake
    * writing to redshift

4. Putting everything together
    * airflow


## Sources: 
* https://www.geodatasource.com/resources/tutorials/international-country-code-fips-versus-iso-3166/

## On top:
* Redshift Spectrum - Query S3


## Run spark-job on emr 3
* bootstrap.sh
* on shell: export PYSPARK_PYTHON=/usr/bin/python3