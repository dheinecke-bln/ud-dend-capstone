0. create S3 bucket and upload files
    * folders google, oxford, gdelt
    * upload files

1. setup spark + emr 
    * setup via script

2. OK: run juypter notebook

3. OK: access file on s3 with Spark
    * use Pyspark kernel
    * see Pyspark_sketches

4. access files in notebook
    * CONTINUE WITH EVERY DATASET
    * transform datatypes
    * join files and check consistency
    * create parquet structure and write to S3
    * create datalake_etl.py and use spark-commit to run it

5. create etl: 
    * process all files and create right schema
    * write real parquets to S3

6. set up redshift and read files (extract) from s3 to redshift
    * using spark
    * https://dwgeek.com/how-to-export-spark-dataframe-to-redshift-table.html/

7. combine everything with airflow

8. connect apis

9. create bi application