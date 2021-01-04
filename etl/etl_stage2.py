import configparser
import logging
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType,StructField, StringType, DateType, IntegerType, DoubleType
from pyspark.sql.functions import col, explode, array, struct, expr, sum, lit
from pyspark.sql.window import Window

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

def process_dim_region(spark):
    """
    Creates the dimension region and stores it to stage 2

    spark: spark session
    """

    # reads index file
    df_index = spark.read.format('csv') \
                    .options(header='true') \
                    .load(folder_s0 + 'google/index.csv')

    # select relevant fields
    df_dim_region = df_index.select("key",\
                                "country_code",\
                                "country_name",\
                                "subregion1_code",\
                                "subregion1_name",\
                                "subregion2_code",\
                                "subregion2_name",\
                                "aggregation_level")
    
    #rename key -> id
    df_dim_region = df_dim_region.withColumnRenamed("key","id")

    #write as dimRegion to stage 2
    df_dim_region.write.mode('overwrite').parquet(folder_s2+'dimRegion.parquet')

    return df_dim_region

def process_dim_time(spark):
    """
    Creates the dimension time and stores it to stage 2.

    Creates a time sequence with a daily interval.

    spark: spark session
    """
   
    #create a daily time sequence starting at 2019-10-01 ending now()
    #src: https://stackoverflow.com/questions/43141671/
    #     sparksql-on-pyspark-how-to-generate-time-series
    df_dim_time = spark.sql("""SELECT sequence(
                                    to_date('2019-10-01'),
                                    now(), 
                            interval 1 day) as date""")\
                        .withColumn("date", explode(col("date")))
    
    #create additional columns e.g. weekofyear
    df_dim_time = df_dim_time.withColumn("month",F.month("date"))\
           .withColumn("year",F.year("date"))\
           .withColumn("dayofmonth",F.dayofmonth("date"))\
           .withColumn("dayofyear",F.dayofyear("date"))\
           .withColumn("weekofyear",F.weekofyear("date"))
    
    #write as dimTime to stage2
    df_dim_time.write.mode('overwrite').parquet(folder_s2+'dimTime.parquet')
    
    return df_dim_time

def determine_symptom_group(s):
    """
    Determines if a given symptom is typical for covid or caused by lockdown
    situations.

    Sources for symptoms:
    - https://www.cdc.gov/coronavirus/2019-ncov/symptoms-testing/symptoms.html
    - https://www.frontiersin.org/articles/10.3389/fpsyg.2020.01491/full
    
    s: symptom

    returns a string either 'covid', 'lockdown' or 'other'
    """

    # create list of symptoms
    covid_symptoms = ['fever', 'chills', 'cough', \
                  'shortness_of_breath', 'shallow_breathing', \
                  'fatigue','headache','sore_throat','throat_irritation',\
                  'nasal_congestion','nausea', 'vomiting', 'diarrhea']
    lockdown_symptoms = ['sleep_deprivation', 'sleep_disorder', 'anxiety', \
                  'generalized_anxiety_disorder', 'panic_attack', 'depression', \
                  'major_depressive_disorder']
    
    # check if symptom belongs to one of the lists
    if s in covid_symptoms:
        return 'covid'
    elif s in lockdown_symptoms:
        return 'lockdown'
    else:
        return 'other'

def process_dim_symptoms(spark):
    """
    Creates the dimension symptom and stores it to stage 2

    spark: spark session
    """
    #create a tmp table using parquet file of stage 1
    #we don't have to read the whole file into memory that way
    stparq_file = folder_s1+"/google/searchtrends.parquet"
    query = """CREATE TEMPORARY VIEW SEARCHTRENDS 
             USING parquet 
             OPTIONS (path \"{0}\")""".format(stparq_file)
    logging.info("query for searchtrends parquet: {0}".format(query))
    spark.sql(query)

    #select distinct symptoms
    df_dim_symptoms = spark.sql("SELECT DISTINCT symptom FROM SEARCHTRENDS")

    # call count to materialize the dataframe
    df_dim_symptoms.count()

    #register the udf to determin the symptom group
    symptgrpFunc = F.udf(determine_symptom_group, StringType())

    #create a new column symptom group by calling the udf
    df_dim_symptoms = df_dim_symptoms \
                     .withColumn("symptom_group",symptgrpFunc(col('symptom')))

    #create a unique id by creating a row number
    df_dim_symptoms = df_dim_symptoms.select(F.row_number() \
                                     .over(Window.partitionBy() \
                                     .orderBy(df_dim_symptoms['symptom'])) \
                                     .alias("id"), "symptom", "symptom_group")

    #write as dimSymptom to stage 2
    df_dim_symptoms.write \
                   .mode('overwrite') \
                   .parquet(folder_s2+'dimSymptom.parquet')

    return df_dim_symptoms

def process_facts_searches(spark, df_dim_symptoms):
    """
    Creates the facts for searches and stores them to stage 2

    spark: spark session
    df_dim_symptoms: spark dataframe of dimension symptoms
    """

    #read searchtrends of stage 1
    df_st = spark.read.parquet(folder_s1+"google/searchtrends.parquet")

    # rename key to regionId
    df_st = df_st.withColumnRenamed("key", "regionId")

    # join with dimsymptom to get id of dimSymptom
    df_st = df_st.join(df_dim_symptoms.select("id","symptom"),on=["symptom"],how="inner")
    df_st = df_st.withColumnRenamed("id", "symptomId")  

    # generate row number
    #df_st = df_st.select(F.row_number()\
    #                 .over(Window.partitionBy()\
    #                 .orderBy(df_st['date'],df_st['regionId']))\
    #                 .alias("id"), "regionId", "date", "symptomId", "search_trend", "country_code")

    #FIXME: generate id

    #write factTable to stage 2
    df_st.write.mode('overwrite') \
               .partitionBy("country_code") \
               .parquet(folder_s2+"/factSearches.parquet")

def read_epidemiology_data(spark):
    """
    Reads the preprocessed epidemiology data of stage 1.
    Selects features of interest and renames the column key
    for upcoming joins.

    spark: spark session

    returns a spark dataframe with epidemiology data
    """
 
    df_epi = spark.read.parquet(folder_s1+"google/epidemiology.parquet")
    df_epi = df_epi.select('key', 'date', 'new_confirmed', 'new_deceased', 'new_recovered', 'new_tested')
    df_epi = df_epi.withColumnRenamed('key','regionId')
    return df_epi

def read_mobility_data(spark):
    """
    Reads the preprocessed mobility data of stage 1.
    Selects features of interest and renames the column key
    for upcoming joins.

    spark: spark session

    returns a spark dataframe with mobility data
    """

    df_mob = spark.read.parquet(folder_s1+"google/mobility.parquet")
    df_mob = df_mob.select('key', 'date', 'mobility_retail_and_recreation', \
                            'mobility_grocery_and_pharmacy', 'mobility_parks', \
                            'mobility_transit_stations','mobility_workplaces',\
                            'mobility_residential')
    df_mob = df_mob.withColumnRenamed('key','regionId')
    return df_mob

def read_oxfordgov_data(spark):
    """
    Reads the preprocessed oxford gov response data of stage 1.
    Selects features of interest and renames the column country_code
    for upcoming joins.

    Filters the data to Jurisdiction = NAT_TOTAL, because the merge
    of subregions is very costly and considered for a later step of the
    project.

    spark: spark session

    returns a spark dataframe with oxford gov response data
    """

    df_oxf = spark.read.parquet(folder_s1+"oxford-gov-response/oxfgovresp.parquet")
    
    # limit to NAT_TOTAL
    df_oxf_f = df_oxf.filter("Jurisdiction == 'NAT_TOTAL'")

    df_oxf_f = df_oxf_f.select("country_code",\
                 "date",\
                 "C1_School_closing",\
                 "C1_Flag",\
                 "C2_Workplace_closing",\
                 "C2_Flag",\
                 "C3_Cancel_public_events",\
                 "C3_Flag",\
                 "C4_Restrictions_on_gatherings",\
                 "C4_Flag",\
                 "C5_Close_public_transport",\
                 "C5_Flag",\
                 "C6_Stay_at_home_requirements",\
                 "C6_Flag",\
                 "C7_Restrictions_on_internal_movement",\
                 "C7_Flag",\
                 "C8_International_travel_controls",\
                 "E1_Income_support",\
                 "E1_Flag",\
                 "E2_Debt_contract_relief",\
                 "E3_Fiscal_measures",\
                 "E4_International_support",\
                 "H1_Public_information_campaigns",\
                 "H1_Flag",\
                 "H2_Testing_policy",\
                 "H3_Contact_tracing",\
                 "H4_Emergency_investment_in_healthcare",\
                 "H5_Investment_in_vaccines",\
                 "H6_Facial_Coverings",\
                 "H6_Flag",\
                 "H7_Vaccination_policy",\
                 "H7_Flag",\
                 "StringencyIndex",\
                 "GovernmentResponseIndex",\
                 "ContainmentHealthIndex",\
                 "EconomicSupportIndex")

    df_oxf_f = df_oxf_f.withColumnRenamed("country_code","regionId")

    return df_oxf_f

def read_and_agg_gdelt_data(spark):
    """
    Reads the preprocessed gdelt data of stage 1.
    
    Creates aggregations by country and date. 
    Creates several metrics, see Readme.md for details.
    
    Merging the subregions 
    is very costly and considered for a later step of the project.

    spark: spark session

    returns a spark dataframe with aggregated gdelt data
    """
    
    #read in gdelt data of stage 1 and create temp view
    df_gdelt = spark.read.parquet(folder_s1+"gdelt/gdelt.parquet")
    df_gdelt.createOrReplaceTempView("gdelt")

    #create df with covid related metrics, grouped by country and date
    df_gdelt_facts_covid = spark.sql("""SELECT country_code, `date`, 
                        COUNT(DISTINCT GLOBALEVENTID) as gd_events_covid,
                        SUM(NumMentions) as gd_nummentions_covid,
                        SUM(NumSources) as gd_numsources_covid,
                        SUM(NumArticles) as gd_numarticles_covid,
                        AVG(AvgTone) as gd_avgtone_covid,
                        AVG(GoldsteinScale) as gd_gtscale_covid
                     FROM gdelt 
                     WHERE covid = true 
                     GROUP BY country_code, `date`
                     """)

    #create df with general metrics, grouped by country and date
    df_gdelt_facts_general = spark.sql("""SELECT country_code, `date`, 
                        COUNT(DISTINCT GLOBALEVENTID) as gd_events_general,
                        SUM(NumMentions) as gd_nummentions_general,
                        SUM(NumSources) as gd_numsources_general,
                        SUM(NumArticles) as gd_numarticles_general,
                        AVG(AvgTone) as gd_avgtone_general,
                        AVG(GoldsteinScale) as gd_gtscale_general
                     FROM gdelt
                     GROUP BY country_code, `date`
                     """)

    #join the two metric frames together
    df_gdelt_facts = df_gdelt_facts_general \
                        .join(df_gdelt_facts_covid, \
                            on=['country_code','date'],how="outer")

    #calculate proportions of covid metric on general metric
    df_gdelt_facts = df_gdelt_facts \
                        .withColumn('gd_events_covid_perc',\
                         col('gd_events_covid') / col('gd_events_general'))
    df_gdelt_facts = df_gdelt_facts \
                        .withColumn('gd_nummentions_covid_perc',\
                         col('gd_nummentions_covid') / col('gd_nummentions_general'))
    df_gdelt_facts = df_gdelt_facts \
                        .withColumn('gd_numsources_covid_perc',\
                         col('gd_numsources_covid') / col('gd_numsources_general'))
    df_gdelt_facts = df_gdelt_facts \
                        .withColumn('gd_numarticles_covid_perc',\
                         col('gd_numarticles_covid') / col('gd_numarticles_general'))                            

    df_gdelt_facts = df_gdelt_facts.withColumnRenamed('country_code', 'regionId')

    return df_gdelt_facts

def process_facts_covid(spark, df_dim_region, df_dim_time):
    """
    Creates the facts of covid data, grouped by date and region.
    Reads the preprocessed data of stage 1 and joins the dataset
    to get the fact table.
    """

    #read datasets
    df_epi = read_epidemiology_data(spark)
    df_mob = read_mobility_data(spark)
    df_oxf = read_oxfordgov_data(spark)
    df_gdelt_facts = read_and_agg_gdelt_data(spark)
    
    #create cartesic product of dimRegion and dimTime
    df_fact_covid = df_dim_region \
                        .crossJoin(df_dim_time) \
                        .select('id','date','country_code')

    #store count before joins for data validation
    factCount = df_fact_covid.count()

    #join with datasets
    df_fact_covid = df_fact_covid.withColumnRenamed('id','regionId')
    df_fact_covid = df_fact_covid.join(df_epi, on=['regionId','date'], how='left')
    df_fact_covid = df_fact_covid.join(df_mob, on=['regionId','date'], how='left')
    df_fact_covid = df_fact_covid.join(df_oxf, on=['regionId','date'], how='left')
    df_fact_covid = df_fact_covid.join(df_gdelt_facts, on=['regionId','date'], how='left')

    #validation of number of records
    factCount_final = df_fact_covid.count()
    assert factCount == factCount_final #FIXME: throw exception

    #validation of number of columns
    len(df_fact_covid.columns) #FIXME: check and throw exception

    #write to stage 2
    df_fact_covid.write \
                 .mode('overwrite') \
                 .partitionBy("country_code") \
                 .parquet(folder_s2+"/factCovid.parquet")

def main():
    """
    main for etl process to read, extract and write
    song and log data
    
    reads data from s3, processes it and writes in back to
    a specified s3 bucket
    """
    spark = create_spark_session()

    #process dimensions
    df_dim_region = process_dim_region(spark)
    df_dim_time = process_dim_time(spark)
    df_dim_symptoms = process_dim_symptoms(spark)

    #process facts
    process_facts_searches(spark, df_dim_symptoms)
    process_facts_covid(spark, df_dim_region, df_dim_time)


if __name__ == "__main__":
    main()