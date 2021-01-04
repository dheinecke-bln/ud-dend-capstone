from pyspark.sql.types import StructType,StructField, StringType, \
                              DateType, IntegerType, DoubleType, \
                              BooleanType, LongType

google_epi_s0_schema = StructType([ \
        StructField("date",DateType(),True), \
        StructField("key",StringType(),True), \
        StructField("new_confirmed",IntegerType(),True), \
        StructField("new_deceased", IntegerType(), True), \
        StructField("new_recovered", IntegerType(), True), \
        StructField("new_tested", IntegerType(), True), \
        StructField("total_confirmed",IntegerType(),True), \
        StructField("total_deceased", IntegerType(), True), \
        StructField("total_recovered", IntegerType(), True), \
        StructField("total_tested", IntegerType(), True), \
    ])

google_mobi_s0_schema = StructType([ \
        StructField("date",DateType(),True), \
        StructField("key",StringType(),True), \
        StructField("mobility_retail_and_recreation",IntegerType(),True), \
        StructField("mobility_grocery_and_pharmacy", IntegerType(), True), \
        StructField("mobility_parks", IntegerType(), True), \
        StructField("mobility_transit_stations", IntegerType(), True), \
        StructField("mobility_workplaces",IntegerType(),True), \
        StructField("mobility_residential", IntegerType(), True), \
    ])

gdelt_s0_schema = StructType() \
            .add("GLOBALEVENTID",LongType(),True) \
            .add("SQLDATE",StringType(),True) \
            .add("MonthYear",IntegerType(),True) \
            .add("Year",IntegerType(),True) \
            .add("FractionDate",StringType(),True) \
            .add("Actor1Code",StringType(),True) \
            .add("Actor1Name",StringType(),True) \
            .add("Actor1CountryCode",StringType(),True) \
            .add("Actor1KnownGroupCode",StringType(),True) \
            .add("Actor1EthnicCode",StringType(),True) \
            .add("Actor1Religion1Code",StringType(),True) \
            .add("Actor1Religion2Code",StringType(),True) \
            .add("Actor1Type1Code",StringType(),True) \
            .add("Actor1Type2Code",StringType(),True) \
            .add("Actor1Type3Code",StringType(),True) \
            .add("Actor2Code",StringType(),True) \
            .add("Actor2Name",StringType(),True) \
            .add("Actor2CountryCode",StringType(),True) \
            .add("Actor2KnownGroupCode",StringType(),True) \
            .add("Actor2EthnicCode",StringType(),True) \
            .add("Actor2Religion1Code",StringType(),True) \
            .add("Actor2Religion2Code",StringType(),True) \
            .add("Actor2Type1Code",StringType(),True) \
            .add("Actor2Type2Code",StringType(),True) \
            .add("Actor2Type3Code",StringType(),True) \
            .add("IsRootEvent",StringType(),True) \
            .add("EventCode",StringType(),True) \
            .add("EventBaseCode",StringType(),True) \
            .add("EventRootCode",StringType(),True) \
            .add("QuadClass",StringType(),True) \
            .add("GoldsteinScale",StringType(),True) \
            .add("NumMentions",IntegerType(),True) \
            .add("NumSources",IntegerType(),True) \
            .add("NumArticles",IntegerType(),True) \
            .add("AvgTone",DoubleType(),True) \
            .add("Actor1Geo_Type",StringType(),True) \
            .add("Actor1Geo_FullName",StringType(),True) \
            .add("Actor1Geo_CountryCode",StringType(),True) \
            .add("Actor1Geo_ADM1Code",StringType(),True) \
            .add("Actor1Geo_Lat",StringType(),True) \
            .add("Actor1Geo_Long",StringType(),True) \
            .add("Actor1Geo_FeatureID",StringType(),True) \
            .add("Actor2Geo_Type",StringType(),True) \
            .add("Actor2Geo_FullName",StringType(),True) \
            .add("Actor2Geo_CountryCode",StringType(),True) \
            .add("Actor2Geo_ADM1Code",StringType(),True) \
            .add("Actor2Geo_Lat",StringType(),True) \
            .add("Actor2Geo_Long",StringType(),True) \
            .add("Actor2Geo_FeatureID",StringType(),True) \
            .add("ActionGeo_Type",StringType(),True) \
            .add("ActionGeo_FullName",StringType(),True) \
            .add("ActionGeo_CountryCode",StringType(),True) \
            .add("ActionGeo_ADM1Code",StringType(),True) \
            .add("ActionGeo_Lat",StringType(),True) \
            .add("ActionGeo_Long",StringType(),True) \
            .add("ActionGeo_FeatureID",StringType(),True) \
            .add("DATEADDED",StringType(),True) \
            .add("SOURCEURL",StringType(),True)

oxford_s0_schema = StructType([ \
        StructField("CountryName", StringType(), True), \
        StructField("CountryCode", StringType(), True), \
        StructField("RegionName", StringType(), True), \
        StructField("RegionCode", StringType(), True), \
        StructField("Jurisdiction", StringType(), True), \
        StructField("date", StringType(), True), \
        StructField("C1_School_closing", DoubleType(), True), \
        StructField("C1_Flag", DoubleType(), True), \
        StructField("C2_Workplace_closing", DoubleType(), True), \
        StructField("C2_Flag", DoubleType(), True), \
        StructField("C3_Cancel_public_events", DoubleType(), True), \
        StructField("C3_Flag", DoubleType(), True), \
        StructField("C4_Restrictions_on_gatherings", DoubleType(), True), \
        StructField("C4_Flag", DoubleType(), True), \
        StructField("C5_Close_public_transport", DoubleType(), True), \
        StructField("C5_Flag", DoubleType(), True), \
        StructField("C6_Stay_at_home_requirements", DoubleType(), True), \
        StructField("C6_Flag", DoubleType(), True), \
        StructField("C7_Restrictions_on_internal_movement", DoubleType(), True), \
        StructField("C7_Flag", DoubleType(), True), \
        StructField("C8_International_travel_controls", DoubleType(), True), \
        StructField("E1_Income_support", DoubleType(), True), \
        StructField("E1_Flag", DoubleType(), True), \
        StructField("E2_Debt_contract_relief", DoubleType(), True), \
        StructField("E3_Fiscal_measures", DoubleType(), True), \
        StructField("E4_International_support", DoubleType(), True), \
        StructField("H1_Public_information_campaigns", DoubleType(), True), \
        StructField("H1_Flag", DoubleType(), True), \
        StructField("H2_Testing_policy", DoubleType(), True), \
        StructField("H3_Contact_tracing", DoubleType(), True), \
        StructField("H4_Emergency_investment_in_healthcare", DoubleType(), True), \
        StructField("H5_Investment_in_vaccines", DoubleType(), True), \
        StructField("H6_Facial_Coverings", DoubleType(), True), \
        StructField("H6_Flag", DoubleType(), True), \
        StructField("H7_Vaccination_policy", DoubleType(), True), \
        StructField("H7_Flag", DoubleType(), True), \
        StructField("M1_Wildcard", DoubleType(), True), \
        StructField("ConfirmedCases", DoubleType(), True), \
        StructField("ConfirmedDeaths", DoubleType(), True), \
        StructField("StringencyIndex", DoubleType(), True), \
        StructField("StringencyIndexForDisplay", DoubleType(), True), \
        StructField("StringencyLegacyIndex", DoubleType(), True), \
        StructField("StringencyLegacyIndexForDisplay", DoubleType(), True), \
        StructField("GovernmentResponseIndex", DoubleType(), True), \
        StructField("GovernmentResponseIndexForDisplay", DoubleType(), True), \
        StructField("ContainmentHealthIndex", DoubleType(), True), \
        StructField("ContainmentHealthIndexForDisplay", DoubleType(), True), \
        StructField("EconomicSupportIndex", DoubleType(), True), \
        StructField("EconomicSupportIndexForDisplay", DoubleType(), True), \
  ])


