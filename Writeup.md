# Covid-19 Capstone Project

2020 brought the Corona-Virus to the world and as sad and dramatic as the pandemic is there is also a lot of public data available. This project uses some of this data to help to analyze the pandemic and its consequences in detail.

## Goal of the Project

The goal of this project is to gather and analyze data of the Corona-Virus pandemic. The project focuses on the dynamic of the pandemic and the response of the people and the public.

#FIXME
* where did it start?
* which effects did the pandemic have on mobility?
* how was it covered in the media?
* which behavioural changes concerning internet search did the pandemic have?
* how did government policies impact the covid cases and vice versa?

## Scope of the Project and used Datasets

The project uses three different data sources and five datasets to build a data lake and/or a data warehouse for analytical purposes.

The data sources and datasets are the following:

* The COVID-19 Open-Data on the Google Cloud Platform
* The Global Database of Events, Language, and Tone - GDELT
* The Oxford Covid-19 Government Response Tracker

### The COVID-19 Open-Data on the Google Cloud Platform

"This repository contains datasets of daily time-series data related to COVID-19 for over 20,000 distinct locations around the world. The data is at the spatial resolution of states/provinces for most regions and at county/municipality resolution for many countries..." [1]

"The data is drawn from multiple sources [...] and stored in separate tables as CSV files grouped by context, which can be easily merged due to the use of consistent geographic (and temporal) keys as it is done for the main table." [2]

Although the Google Cloud Platform offers many dataset we will only used three datasets, because we are mainly interested in the response of the people and the public:

* The Epidemiology Dataset
* The Mobility Dataset
* The Google Search Trends Dataset

The data and the documention of the COVID-19 Open-Data can be found here: https://github.com/GoogleCloudPlatform/covid-19-open-data/blob/main/README.md

### The Global Database of Events, Language, and Tone - GDELT

"[...] the GDELT Project monitors the world's broadcast, print, and web news from nearly every corner of every country in over 100 languages and identifies the people, locations, organizations, themes, sources, emotions, counts, quotes, images and events driving our global society every second of every day, creating a free open platform for computing on the entire world." [3]

For this project data of the GDELT 1.0 Event Database is used. See: https://www.gdeltproject.org/data.html

The data is very massive, containing hundreds of thousands of records for every day. To download an example batch of the data go to: http://data.gdeltproject.org/events/index.html

### The Oxford Covid-19 Government Response Tracker

"The Oxford Covid-19 Government Response Tracker (OxCGRT) collects systematic information on which governments have taken which measures, and when. This can help decision-makers and citizens understand governmental responses in a consistent way, aiding efforts to fight the pandemic. The OxCGRT systematically collects information on several different common policy responses governments have taken, records these policies on a scale to reflect the extent of government action, and aggregates these scores into a suite of policy indices." [4]

For more details see: https://github.com/OxCGRT/covid-policy-tracker

The data can be found here: https://github.com/OxCGRT/covid-policy-tracker/blob/master/data/OxCGRT_latest.csv


## Assessment of the datasets

    Short summary of datasets, reference to notebooks, Spark on EMR

The assessment of the datasets was done with Apache Spark on AWS EMR. For details see the Jupyter Notebook in the Folder [Assessment](/assessment)

## Data Architecture

    Include a description of how you would approach the problem differently under the following scenarios:
        If the data was increased by 100x.
        If the pipelines were run on a daily basis by 7am.
        If the database needed to be accessed by 100+ people.

### Ingestion
* status quo: download and upload to S3-bucket
* gdelt: only one file
* not updated
* in the future: apis, see outlook

### Stages
* Stage 0: raw data
* Stage 1: preprocessed, cleaned and normalized data
* Stage 2: joined data and represented in the data model

### Data Model
* picture and explain it
    Map out the conceptual data model and explain why you chose that model
    List the steps necessary to pipeline the data into the chosen data model
#### Data Dictionary
* reference to separate md file

### Analytical components
* any solution that can process S3 parquet files
* spark on EMR, Redshift Spectrum, Athena etc.
* future: Quicksight on Redshift Spectrum or cost-effict app with Plotly

## Data Processing

    How would Spark or Airflow be incorporated? Why did you choose the model you chose?
    Clearly state the rationale for the choice of tools and technologies for the project.

    Create the data pipelines and the data model
    Include a data dictionary
    Run data quality checks to ensure the pipeline ran as expected
        Integrity constraints on the relational database (e.g., unique key, data type, etc.)
        Unit tests for the scripts to ensure they are doing the right thing
        Source/count checks to ensure completeness
    Propose how often the data should be updated and why.

### Stage 0 - raw data

### Stage 1 - preprocessed, cleaned and normalized data

### Stage 2 - joined data and represented in the data model

## Outlook and next steps

    Post your write-up and final data model in a GitHub repo.



[1]: https://github.com/GoogleCloudPlatform/covid-19-open-data/blob/main/README.md
[2]: https://github.com/GoogleCloudPlatform/covid-19-open-data/blob/main/README.md
[3]: https://www.gdeltproject.org/
[4]: https://github.com/OxCGRT/covid-policy-tracker