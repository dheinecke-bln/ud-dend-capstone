# Technical setup to run the notebooks and ETL-Processes

This document describes the steps to be taken to run the etl processes or Jupyter Notebooks (optional) on AWS.

## Create S3 bucket and folders and upload files

Create an S3 bucket on AWS. Download the data of the data folder and upload it to the newly created bucket.

**Note**: place the data in a folder named `stage0_raw`

**Note**: because the files `epidemiology.csv` and `google-search-trends.csv` are too large for github, please download them manually from [here](https://storage.googleapis.com/covid19-open-data/v2/epidemiology.csv) and [here](https://storage.googleapis.com/covid19-open-data/v2/google-search-trends.csv) and upload them to your bucket in the folder `stage0_raw/google`.

Copy the file `misc/bootstrap.sh` to a S3 location of your choice.

## Setup AWS EMR Cluster

Create an EMR-Cluster on AWS. The EMR version used in this project is `emr-5.29.0`.

Use the following EMR components:

* Hadoop
* Spark
* Livy
* Hive

Define a bootstrap action and refer to the file `bootstrap.sh` which you have uploaded to S3 before. The purpose of the bootstrap file is to install python libs, configparser in this case.

Make sure you create an EC2-keypair and specify it in the cluster creation process.

## Create and Run Jupyter Notebook on EMR (optional)

To run the Jupyter Notebooks for data assessment you have to do the following steps
* Create a Jupyter Notebook in EMR
* Choose the newly created cluster
* Define a S3 location for the notebooks (.ipynb files)
* Copy the ipynb files (notebooks) of the assessment folder to this S3 location
* Open the notebooks
* Replace your bucket name in notebooks and run them

## Run the ETL Spark Jobs on EMR

There are many options how to run a spark job on AWS EMR. You can use AWS CLI or the EMR GUI or connect directly to the master via ssh and run the jobs there. The last option is described here in more detail:

* Connect to the master with putty. You'll need the key pair associated with the cluster and the url of the cluster. See the AWS documentation for more details.
* When connected, copy the content of the folder `etl` to the master.
* Specify the S3 bucket which holds the data and your aws credentials in `etl.cfg`.
* On the shell of the master run `export PYSPARK_PYTHON=/usr/bin/python3` to make sure the job runs with Python3.
* First run `spark-submit etl_stage1.py`
* Then run `spark-submit etl_stage2.py`