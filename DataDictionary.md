# Data Dictionary

## dimTime

| Name        | Type           | Description  |   Not Null    |   Example     |
| ------------- |:-------------------|:-------------|:-------------:|:----------------------|
| date          | date               | Date, without time, works as primary key        |True          |2020-10-03                    |
| year          | integer                |   Year of the given Date        |True          |2020                    |
| month         | integer                |    Month of the given Date        |True          |10                    |
| dayofmonth    | integer                |    Day of Month of the given Date        |True          |3                    |
| dayofyear     | integer                |    Day of Year of the given Date        |True          |278                    |
| weekofyear    | integer                |    Week of Year of the given Date        |True          |41                    |



## dimRegion

see also [google cloud platform covid data index file](https://github.com/GoogleCloudPlatform/covid-19-open-data/blob/main/docs/table-index.md)

| Name        | Type           | Description  |   Not Null    |   Example     |
| ------------- |:-------------------|:-------------|:-------------:|:----------------------|
| id          | varchar               | Unique string identifying the region, works as primary key        |True          |US_CA_06001                    |
| country_code          | varchar                |   ISO 3166-1 alphanumeric 2-letter code of the country        |True          |US                    |
| country_name         | varchar                |    Name of the country       |True          |United States of America                    |
| subregion1_code    | varchar                |    ISO 3166-2 or NUTS 2/3 code of the subregion        |False          |CA                    |
| subregion1_name     | varchar                |    Name of the subregion1        |False          |California                   |
| subregion2_code    | varchar                |    FIPS code of the county (or local equivalent)        |False          |06001                   |
| subregion2_name    | varchar                |    Name of the subregion2        |False          |Alameda County                    |
| aggregation_level    | integer [0-2]                |    Level at which data is aggregated, i.e. country, state/province or county level        |True          |2                    |


## dimSymptom

| Name        | Type           | Description  |   Not Null    |   Example     |
| ------------- |:-------------------|:-------------|:-------------:|:----------------------|
| id          | integer               | Unique identifier, works as primary key        |True          |514                    |
| symptom          | varchar                |   Name of a medical symptom        |True          |Shortness of Breath                    |
| symptom_group         | varchar                |    Group of the medical symptom, related to Covid-19 <br /> 'covid'=Covid-Symptom, <br /> 'lockdown'=Symptom related to lockdowns <br />  'other'=Other symptom      |True          |10                    |

## factSearches

| Name        | Type           | Description  |   Not Null    |   Example     |
| ------------- |:-------------------|:-------------|:-------------:|:----------------------|
| id          | varchar               | Unique and composed identifier, works as primary key        |True          |514-2020-10-12-US                    |
| date          | date                |   Date of the search index, reference to dimTime        |True          |2020-10-12                    |
| regionId         | varchar                |    Region of the search index, reference to dimRegion      |True          |US                    |
| symptomId         | int                |    Symptom searched for, reference to dimSymptom      |True          |514                    |
| searchIndex         | double               |    Reflects the normalized search volume for this symptom, for the specified date and region      |False          |87.02                  |

## factCovid

see also [google cloud platform covid data epidemiology](https://github.com/GoogleCloudPlatform/covid-19-open-data/blob/main/docs/table-index.md)

see also [google cloud platform covid data mobility](https://github.com/GoogleCloudPlatform/covid-19-open-data/blob/main/docs/table-index.md)

see also [google cloud platform covid data search trends](https://github.com/GoogleCloudPlatform/covid-19-open-data/blob/main/docs/table-index.md)

see also [oxford policy tracker](https://github.com/OxCGRT/covid-policy-tracker/tree/master/documentation)

see also [gdelt documentation](https://www.gdeltproject.org/data.html#documentation)

| Name        | Type           | Description  |   Not Null    |   Example     |
| ------------- |:-------------------|:-------------|:-------------:|:----------------------|
| id          | varchar               | Unique and composed identifier, works as primary key        |True          |26564                    |
| regionId| string | String identifying the region, referencing dimRegion        |True          |US_CA_06001                    |
| date| timestamp | Date (day format), referencing dimTime        |True          |2020-10-12                    |
| new_confirmed| integer | Count of new cases confirmed after positive test on this date        |False          |34                   |
| new_deceased| integer | Count of new deaths from a positive COVID-19 case on this date        |False          |2                    |
| new_recovered| integer | Count of new recoveries from a positive COVID-19 case on this date        |False          |13                    |
| new_tested| integer | Count of new COVID-19 tests performed on this date       |False          |13                    |
| mobility_retail_and_recreation| integer | Percentage change in visits to restaurants, cafes, shopping centers, theme parks, museums, libraries, and movie theaters compared to baseline        |False          |3                    |
| mobility_grocery_and_pharmacy| integer | Percentage change in visits to places like grocery markets, food warehouses, farmers markets, specialty food shops, drug stores, and pharmacies compared to baseline        |False          |3                    |
| mobility_parks| integer | Percentage change in visits to places like local parks, national parks, public beaches, marinas, dog parks, plazas, and public gardens compared to baseline        |False          |3                    |
| mobility_transit_stations| integer | Percentage change in visits to places like public transport hubs such as subway, bus, and train stations compared to baseline        |False          |3                    |
| mobility_workplaces| integer | Percentage change in visits to places of work compared to baseline        |False          |3                    |
| mobility_residential| integer | Percentage change in visits to places of residence compared to baseline       |False          |3                    |
| C1_School_closing| integer [0-3]| Record closings of schools and universities        |False          |0                    |
| C1_Flag| integer [0-1]| Binary flag for geographic scope        |False          |1                    |
| C2_Workplace_closing| integer [0-3] | Record closings of workplaces	 |False          |1                   |
| C2_Flag| integer [0-1]| Binary flag for geographic scope        |False          |1                    |
| C3_Cancel_public_events| integer [0-3] | Record cancelling public events	 |False          |1                   |
| C3_Flag| integer [0-1]| Binary flag for geographic scope        |False          |1                    |
| C4_Restrictions_on_gatherings| integer [0-3] | Record limits on private gatherings |False          |1                   |
| C4_Flag| integer [0-1]| Binary flag for geographic scope        |False          |1                    |
| C5_Close_public_transport| integer [0-3] | Record closing of public transport |False          |1                   |
| C5_Flag| integer [0-1]| Binary flag for geographic scope        |False          |1                    |
| C6_Stay_at_home_requirements| integer [0-3] | Record orders to "shelter-in-place" and otherwise confine to the home	 |False          |1                   |
| C6_Flag| integer [0-1]| Binary flag for geographic scope        |False          |1                    |
| C7_Restrictions_on_internal_movement| integer [0-3] | Record restrictions on internal movement between cities/regions	 |False          |1                   |
| C7_Flag| integer [0-1]| Binary flag for geographic scope        |False          |1                    |
| C8_International_travel_controls| integer [0-3] | Record restrictions on international travel	 |False          |1                   |
| E1_Income_support| integer [0-2]| Record if the government is providing direct cash payments to people who lose their jobs or cannot work.       |False          |2                   |
| E1_Flag| integer [0-1]| Binary flag for geographic scope        |False          |1                    |
| E2_Debt_contract_relief| integer [0-2] | Record if the government is freezing financial obligations for households (eg stopping loan repayments, preventing services like water from stopping, or banning evictions)       |False          |2                    |
| E3_Fiscal_measures| double [USD]| Announced economic stimulus spending        |False          |200.000 USD                    |
| E4_International_support| double [USD] | Announced offers of Covid-19 related aid spending to other countries       |False          |2000000 USD                    |
| H1_Public_information_campaigns| integer [0-2]| Record presence of public info campaigns        |False          |1                    |
| H1_Flag| integer [0-1]| Binary flag for geographic scope        |False          |1                    |
| H2_Testing_policy| integer [0-3]| Record government policy on who has access to testing        |False          |1                    |
| H3_Contact_tracing| integer [0-2]| Record government policy on contact tracing after a positive diagnosis        |False          |1                    |
| H4_Emergency_investment_in_healthcare| double [USD]| Announced short term spending on healthcare system, eg hospitals, masks, etc       |False          |1000000 USD                    |
| H5_Investment_in_vaccines| double [USD]| Announced public spending on Covid-19 vaccine development        |False          |1000000 USD                    |
| H6_Facial_Coverings| integer [0-4] | Record policies on the use of facial coverings outside the home        |False          |1                    |
| H6_Flag| integer [0-1]| Binary flag for geographic scope        |False          |1                    |
| H7_Vaccination_policy| integer [0-5] | Record policies for vaccine delivery for different groups        |False          |1                    |
| H7_Flag| integer [0-1]| Binary flag for geographic scope        |False          |1                    |
| StringencyIndex| double [0-100]| Overall index        |False          |0.87                 |
| GovernmentResponseIndex| double [0-100]| Government response index        |False          |0.74                    |
| ContainmentHealthIndex| double [0-100]| Containment and health index        |False          |0.53                    |
| EconomicSupportIndex| double [0-100]| Economic support index        |False          |0.89                    |
| gd_events_general| long | Number of recorded events on that date        |False          |1515                    |
| gd_nummentions_general| long | Number of recorded mentions on that date        |False          |1845                    |
| gd_numsources_general| long | Number of recorded sources on that date        |False          |955                    |
| gd_numarticles_general| long | Number of recorded articles on that date        |False          |1994                  |
| gd_avgtone_general| double | Average “tone” of all documents containing one or more mentions. The score ranges from -100 (extremely negative) to +100 (extremely positive). Common values range between -10 and +10, with 0 indicating neutral.        |False          |-5.7                    |
| gd_gtscale_general| double | Average Goldstein Scale |False          |9.8                    |
| gd_events_covid| long | Number of recorded COVID events on that date         |False          |254                    |
| gd_nummentions_covid| long | Number of recorded COVID mentions on that date        |False          |306                    |
| gd_numsources_covid| long | Number of recorded COVID sources on that date        |False          |411                    |
| gd_numarticles_covid| long | Number of recorded COVID articles on that date        |False          |584                    |
| gd_avgtone_covid| double | Average “tone” of all COVID documents containing one or more mentions. The score ranges from -100 (extremely negative) to +100 (extremely positive). Common values range between -10 and +10, with 0 indicating neutral.        |False          |-20.41                    |
| gd_gtscale_covid| double | Average Goldstein Scale for COVID events  |False          |5.6                    |
| gd_events_covid_perc| double | Proportion of COVID events of all events      |False          |0.14                    |
| gd_nummentions_covid_perc| double | Proportion of COVID mentions of all mentions        |False          |0.36                    |
| gd_numsources_covid_perc| double | Proportion of COVID sources of all sources         |False          |0.41                    |
| gd_numarticles_covid_perc| double | Proportion of COVID articles of all articles         |False          |0.48                    |
| country_code| string | ISO 3166-1 alphanumeric 2-letter code of the country        |True          |US                    |