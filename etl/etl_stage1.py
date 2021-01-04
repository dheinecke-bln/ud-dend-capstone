import configparser
import logging
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType,StructField, StringType, \
                              DateType, IntegerType, DoubleType, \
                              BooleanType, LongType
from pyspark.sql.functions import col, explode, array, struct, expr, sum, lit
from pyspark.sql.window import Window

import schema_helper

# read in config
config = configparser.ConfigParser()
config.read('etl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

s3bucket = config['S3']['bucket']
folder_s0 = s3bucket + config['S3']['folder_s0']
folder_s1 = s3bucket + config['S3']['folder_s1']
folder_s2 = s3bucket + config['S3']['folder_s2']

def create_spark_session():
    """
    Creates a spark session, set aws library and specifies algorithm version
    to improve etl performance
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    spark.conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
    return spark

def read_google_index(spark, full=False):
    """
    Reads in google index file and returns it.

    spark: spark session
    full:   True if all columns should be returned, 
            False if limited to key and country_code

    returns: index file
    """
    #read file of stage 0
    df_index = spark.read.format('csv') \
                .options(header='true') \
                .load(folder_s0 + 'google/index.csv')
    
    #limit columns if necessary
    if full == False:
        df_index = df_index.select('key', 'country_code')
    
    return df_index

def process_epidemiology_data(spark, df_index):
    """
    Processes epidemiology data. Reads in data
    with defined schema, adds month and year column 
    and joins with index data.

    spark: spark session
    df_index: index file
    """

    #get schema
    schema = schema_helper.google_epi_s0_schema

    #read raw data with defined schema
    df_epi = spark.read \
                .format('csv') \
                .options(header='true') \
                .schema(schema) \
                .load(folder_s0 + 'google/epidemiology.csv')

    #add columns year and month
    df_epi = df_epi \
                .withColumn("month",F.month("date")) \
                .withColumn("year",F.year("date"))

    #join with index
    df_epi_with_index = df_epi.join(df_index, on=['key'], how='inner')

    #write cleanded frame to stage 1 partitioned by country_code
    df_epi_with_index. \
            write.mode('overwrite') \
            .partitionBy("country_code") \
            .parquet(folder_s1+"google/epidemiology.parquet")

def process_mobility_data(spark, df_index):
    """
    Processes mobility data. Reads in data
    with defined schema, adds month and year column 
    and joins with index data.

    spark: spark session
    df_index: index file
    """

    #get schema
    schema_mobility = schema_helper.google_mobi_s0_schema

    #read raw data with defined schema
    df_mob = spark.read.format('csv') \
                    .options(header='true') \
                    .schema(schema_mobility) \
                    .load(folder_s0 + 'google/mobility.csv')

    #add columns year and month
    df_mob = df_mob.withColumn("month",F.month("date")).withColumn("year",F.year("date"))
    
    #join with ind
    df_mob_with_index = df_mob.join(df_index, on=['key'], how='inner')
    
    #write cleanded frame to stage 1 partitioned by country_code
    df_mob_with_index.write.mode('overwrite') \
                        .partitionBy("country_code") \
                        .parquet(folder_s1+"google/mobility.parquet")

def transpose_search_trends(df, by):
    """
    Transposes symptoms of search trends dataset from columns to rows.

    df: search trend dataframe
    by: columns to keep

    src for transpose:
    https://stackoverflow.com/questions/55378047/pyspark-dataframe-melt-columns-into-rows

    returns: transposed dataframe
    """
    cols, dtypes = zip(*((c, t) for (c, t) in df.dtypes if c not in by))
    kvs = explode(array([
      struct(lit(c).alias("symptom"), col(c).alias("search_trend")) for c in cols
    ])).alias("kvs")
    return df.select(by + [kvs]).select(by + ["kvs.symptom", "kvs.search_trend"])

def process_search_trends(spark, df_index):
    """
    Processes search trends data. Reads in data,
    transposes the symptom columns (more than 400 columns)
    to rows and converts the data types of date and search_trend.

    spark: spark session
    df_index: index file
    """

    #read in file
    df_st = spark.read.format('csv') \
                    .options(header='true') \
                    .load(folder_s0 + 'google/google-search-trends.csv')
    
    #transpose symptom columns to rows
    df_st_tr = transpose_search_trends(df_st, ['date', 'key'])

    #asserts the number of columns are still correct
    assert ((len(df_st.columns)-2) * df_st.count()) == df_st_tr.count()

    # replace search_trend in symptom
    df_st_tr = df_st_tr.withColumn('symptom',expr('substring(symptom, 15)'))

    # convert date to date
    df_st_tr = df_st_tr.withColumn("date", df_st_tr["date"].cast("date"))

    # convert search_trend to double
    df_st_tr = df_st_tr. \
                withColumn("search_trend", df_st_tr["search_trend"].cast("double"))

    df_st_tr = df_st_tr. \
                withColumn("search_trend", df_st_tr["search_trend"].cast("double"))

    # join with index
    df_st_tr = df_st_tr.join(df_index, on=['key'], how='inner')

    # partion by country
    df_st_tr.write.mode('overwrite') \
                    .partitionBy("country_code") \
                    .parquet(folder_s1+"google/searchtrends.parquet")


def process_google_data(spark):
    """
    Processes the google datasets
    """
    df_index = read_google_index(spark)

    process_epidemiology_data(spark, df_index)
    process_mobility_data(spark, df_index)
    process_search_trends(spark, df_index)


def read_fips2iso_mapping(spark):
    """
    Reads and prepares mapping file from FIPS 10-4 country code
    to ISO 3166-1. Mapping file should be located in folder
    with stage0 data.

    #source for mappings:
    https://www.geodatasource.com/resources/tutorials/
        international-country-code-fips-versus-iso-3166/

    spark: spark session

    returns: mapping dataframe
    """

    #read in file    
    mapping_file = folder_s0+'mapping_fips_to_iso3166.csv'
    df_mapping = spark.read.option("delimiter", "\t") \
                    .csv(mapping_file,header=True)

    #select columns and rename them    
    df_mapping = df_mapping.select('FIPS 10-4 ', 'ISO 3166-1') 
    df_mapping = df_mapping \
                    .withColumnRenamed("FIPS 10-4 ","ActionGeo_CountryCode") \
                    .withColumnRenamed("ISO 3166-1","country_code")
    
    #strip values
    stripFunc = F.udf(lambda x: x.strip(),StringType())
    df_mapping = df_mapping \
                    .withColumn('ActionGeo_CountryCode', \
                            stripFunc(col('ActionGeo_CountryCode'))) \
                    .withColumn('country_code', stripFunc(col('country_code')))

    return df_mapping

def containsCovidContent(url):
    """
    Determines if an url contains covid content.
    Basically checks for certain keywords.

    url: url to check

    returns True if url contains any of the keywords
    """
    if url is None:
        return False
    if 'covid' in url.lower():
        return True
    if 'corona' in url.lower():
        return True
    if 'virus' in url.lower():
        return True
    
    return False

def process_gdelt_data(spark, datestr):
    """
    Processes the gdelt dataset by day.

    Transforms datatypes, add columns for month.

    Reduces the dataset to the necessary columns.

    Maps the country code to the ISO code which is used in the
    google and oxford datasets.

    Determines if url contains covid related content.

    Writes the cleaned data back to S3 partitioned by country code.

    spark: spark session
    datestr: day to be processed, needs to be a string in the format YYYYMMDD
    """
    
    # get schema of gdelt data
    schema = schema_helper.gdelt_s0_schema

    # read in original data from S3
    gdelt_file = 'gdelt/'+datestr+'.export.CSV'
    df_gdelt = spark.read.option("delimiter", "\t") \
                .csv(folder_s0 + gdelt_file,header=False,schema=schema)

    # converte date column to date type
    datefunc = F.udf(lambda x: datetime.strptime(x, '%Y%m%d'), DateType())
    df_gdelt = df_gdelt.withColumn('date', datefunc(col('SQLDATE')))
    #create month column
    df_gdelt = df_gdelt.withColumn("month",F.month("date"))

    #reduce dataset by selecting specific columns
    df_gdelt_reduced = df_gdelt.select("GLOBALEVENTID",\
                                    "date",\
                                    "month",\
                                    "Year",\
                                    "Actor1Code",\
                                    "Actor1Name",\
                                    "Actor1CountryCode",\
                                    "Actor2Code",\
                                    "Actor2Name",\
                                    "Actor2CountryCode",\
                                    "IsRootEvent",\
                                    "EventCode",\
                                    "EventBaseCode",\
                                    "EventRootCode",\
                                    "QuadClass",\
                                    "GoldsteinScale",\
                                    "NumMentions",\
                                    "NumSources",\
                                    "NumArticles",\
                                    "AvgTone",\
                                    "ActionGeo_Type",\
                                    "ActionGeo_CountryCode",\
                                    "ActionGeo_ADM1Code",\
                                    "SOURCEURL")

    #determines if url contains covid related content
    #and creates column covid (True=contains keywords)
    covidFunc = F.udf(lambda x: containsCovidContent(x), BooleanType())
    df_gdelt_reduced = df_gdelt_reduced. \
                        withColumn('covid', covidFunc(col('SOURCEURL')))
    
    #rename column Year to year
    df_gdelt_reduced = df_gdelt_reduced.withColumnRenamed("Year","year")

    #maps fips based country code to iso
    df_mapping = read_fips2iso_mapping(spark)
    df_gdelt_reduced = df_gdelt_reduced \
                        .join(df_mapping, \
                            on=['ActionGeo_CountryCode'], how='left')

    #write data back to S3 partitioned by country_code
    df_gdelt_reduced.write.mode('overwrite') \
                        .partitionBy("country_code") \
                        .parquet(folder_s1+"gdelt/gdelt.parquet")

def cast2type(df, field, casttype):
    """
    Helper function to cast a column to a specific type

    df: dataframe
    field: field/column to convert
    casttype: type to cast the type to

    returns dataframe with converted column
    """
    return df.withColumn(field, df[field].cast(casttype))

def process_oxford_data(spark):
    """
    Processes the oxford government response data.

    Transforms datatypes, add columns for month, year.

    Reduces the dataset to the necessary columns.

    Maps the country code to the ISO 3166-1-alpha-2 code which is used in the
    google datasets.

    Writes the cleaned data back to S3 partitioned by country code.

    spark: spark session
    """

    #reads in schema of the oxford data
    schema = schema_helper.oxford_s0_schema

    #reads dataset from S3
    df_ox = spark.read.schema(schema).\
            csv(folder_s0 + 'oxford-gov-response/OxCGRT_latest.csv',\
                header=True)

    #converts types
    datefunc = F.udf(lambda x: datetime.strptime(x, '%Y%m%d'), DateType())
    df_ox = df_ox.withColumn('date', datefunc(col('date')))
    df_ox = df_ox.withColumn("month",F.month("date"))
    df_ox = df_ox.withColumn("year",F.year("date"))

    #arrays with columns that needs to be converted to integer
    flds_cast2int = [
        "C1_School_closing", \
        "C1_Flag", \
        "C2_Workplace_closing", \
        "C2_Flag", \
        "C3_Cancel_public_events",  \
        "C3_Flag", \
        "C4_Restrictions_on_gatherings", \
        "C4_Flag",  \
        "C5_Close_public_transport", \
        "C5_Flag",  \
        "C6_Stay_at_home_requirements", \
        "C6_Flag", \
        "C7_Restrictions_on_internal_movement",  \
        "C7_Flag", \
        "C8_International_travel_controls", \
        "E1_Income_support", \
        "E1_Flag",  \
        "E2_Debt_contract_relief", \
        "H1_Public_information_campaigns",  \
        "H1_Flag", \
        "H2_Testing_policy",  \
        "H3_Contact_tracing",  \
        "H6_Facial_Coverings",  \
        "H6_Flag",  \
        "H7_Vaccination_policy",  \
        "H7_Flag"
    ]

    #convert columns in array to integer
    for f in flds_cast2int:
        df_ox = cast2type(df_ox, f, "integer")

    #read in google index data and select only the iso country encodings
    df_gl_index = read_google_index(spark, full=True)
    google_cc = df_gl_index \
                .select('3166-1-alpha-2', '3166-1-alpha-3') \
                .distinct()

    #join oxford data with country code list
    df_ox = df_ox.alias('o') \
                .join(google_cc.alias('g'), \
                    on=[col('o.CountryCode') == col('g.3166-1-alpha-3')], \
                     how='left')

    #rename joined column to country_code
    df_ox = df_ox.withColumnRenamed("3166-1-alpha-2","country_code")

    #reduce columns
    df_ox_reduced = df_ox.select("country_code", \
                 "date", \
                 "month", \
                 "year", \
                 "RegionName", \
                 "RegionCode", \
                 "Jurisdiction", \
                 "C1_School_closing", \
                 "C1_Flag", \
                 "C2_Workplace_closing", \
                 "C2_Flag", \
                 "C3_Cancel_public_events", \
                 "C3_Flag", \
                 "C4_Restrictions_on_gatherings", \
                 "C4_Flag", \
                 "C5_Close_public_transport", \
                 "C5_Flag", \
                 "C6_Stay_at_home_requirements", \
                 "C6_Flag", \
                 "C7_Restrictions_on_internal_movement", \
                 "C7_Flag", \
                 "C8_International_travel_controls", \
                 "E1_Income_support", \
                 "E1_Flag", \
                 "E2_Debt_contract_relief", \
                 "E3_Fiscal_measures", \
                 "E4_International_support", \
                 "H1_Public_information_campaigns", \
                 "H1_Flag", \
                 "H2_Testing_policy", \
                 "H3_Contact_tracing", \
                 "H4_Emergency_investment_in_healthcare", \
                 "H5_Investment_in_vaccines", \
                 "H6_Facial_Coverings", \
                 "H6_Flag", \
                 "H7_Vaccination_policy", \
                 "H7_Flag", \
                 "StringencyIndex", \
                 "GovernmentResponseIndex", \
                 "ContainmentHealthIndex", \
                 "EconomicSupportIndex")

    #write data back to S3 partitioned by country_code
    df_ox_reduced.write.mode('overwrite') \
                .partitionBy("country_code") \
                .parquet(folder_s1+"oxford-gov-response/oxfgovresp.parquet")

def main():
    """
    main for etl process to read, extract and write
    the datasets to stage 1
    
    reads data from s3, processes it and writes in back to
    a specified s3 bucket
    """
    spark = create_spark_session()

    process_google_data(spark)
    process_gdelt_data(spark, '20201215')
    process_oxford_data(spark)

if __name__ == "__main__":
    main()