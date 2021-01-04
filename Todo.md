* TODO: readme schreiben
* TODO: writeup strukturieren
* TODO: update schema
* TODO: create dict as md-File

* TODO CODING
* FIXME in etl2: id for factSymptoms erzeugen
* etl1: day by day processment of gdelt
* data validations implementieren
* use config in notebooks or put placeholder for s3-bucket
* FIXME in general


* OPTIONAL: schreiben nach Redshift

0. create S3 bucket and upload files
    * folders google, oxford, gdelt
    * upload files

1. OK: setup spark + emr
    * EMR version emr-5.29.0
    * Hadoop, Spark, Livy, Hive
    * works with capstone_3 (j-3QFH9TUF3Z6U8)

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


# EXAMPLE QUERIES:

* df_epi_pq_de = spark.read.parquet("s3://dh-ud-dend-capstone/stage1_prepared_and_partitioned/google/epidemiology.parquet/country_code=DE")