# Data-Engineering-ND
Ever since the dawn of human civilization, man has been moving from place to place in search of food, water, shelter. etc. In the modern world, country borders, for most parts, are clearly demarcated, which means movement of men and material is not as simple as walking across the border; this is where the more formal term - Immigrations, comes in. It is the act of coming to live in another person's country for various reasons. These checkpoints could be at airports, seaports or land borders. Here government officials check documents to ensure the traveler is carrying all necessary legal documents for his/her stay in the country.

The data provided for this project is from the US I94 Immigrations 2016 time period. According to [Wikipedia](https://en.wikipedia.org/wiki/Immigration_to_the_United_States#:~:text=According%20to%20the%202016%20Yearbook,565k%20status%20adjustments) - the 2016 Yearbook of Immigration Statistics, the United States admitted a total of **1.18 million** legal immigrants. The consequences are plenty: from adding to the population to adding to the skilled workforce, and from increasing crime rate to burdening social securiy.

The analysis of such data along with additional sources such as the US census demographic data can help Immigrations officials better deal with the influx of visitors thronging their country every day.

# Data sources
## 1. U.S. I94 immigrations data 2016
This is the main data source, and it has 12 files - one for each month of the  given year; the files are stored in sas7bdat format, and each file has roughly 3-4 million records for a total size of ~6GB. The data comes from here - [The US National Tourism and Trade Office](https://travel.trade.gov/research/reports/i94/historical/2016.html). A data dictionary is also provided to supplement these data.

## 2. U.S. Cities demographics 2015
This dataset contains information about the demographics of all US cities and census-designated places with a population greater or equal to 65,000. 
This data comes from the US Census Bureau's 2015 American Community Survey. Here is the source [OpendDataSoft](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/information/)

## 3. Airport codes
This is a simple table of airport codes and corresponding cities. It comes from [DataHub](https://datahub.io/core/airport-codes#data)

# Purpose of the data
As the data is dated, the analysis coming out of it, at best will give you a picture of the immigration trends in 2016. However, the end-usesrs can purchase year on year data, to help answer some pertinent question, which can help several institutions aside from the Immigrations authorities with planning and execution of various projects:
1) What does the immigrations trend look like? What is the yearly growth rate?
2) Which countries see the vast majority of visitors flowing into the U.S. yearly?
3) Which cities receives the majority of these visitors yearly?
4) What is the favourite mode of transportation? Do some visitors prefer a more obvious mode given geographical closeness?
5) Which months are generally preferred for visits?
6) WHich visa type is the most popular?
7) What is Age/Work/Gender demographics of the travelers?
8) What is the purpose of their visit?
9) On average, what is the duration of stay?
10) Which airports are the busiest during the year?

The data can also be mashed with the U.S. census data to study the impact on the accuracy of the Census numbers.

## Technologies used
![image](https://user-images.githubusercontent.com/7212518/117294794-b0d1df80-ae90-11eb-8e53-e617a01cb67c.png) ![image](https://user-images.githubusercontent.com/7212518/117294934-dced6080-ae90-11eb-908d-36abdf7b994b.png)

- Airflow is an open-sourced Directed Acyclic Graph(DAG) based, schedulable, data-pipeline orchestration tool. Developers can build dags using Python, and schedule them; the jobs can be monitored via a user-friendly UI.
- PySpark is an interface for Apache Spark in Python. All the heavy lifting is managed by Pyspark APIs as it offers in-memory computing capabilities.
- AWS S3 - Amazon S3 or Amazon Simple Storage Service is a service offered by Amazon Web Services that provides object storage through a web service interface.

# Data model
The model architected to support this data source is a snowflake schema; this is a multidimensional model, which has its own fact table along with dimension tables having their own sub-dimensions. The model supports both normalization and denormalization, and it takes up less space relative to the star schema. On the flip side, the queries might be a tad more complex.

![image](https://user-images.githubusercontent.com/7212518/117299326-078de800-ae96-11eb-8984-a3fd40dfab01.png)

There are 3 main sources of data; the I94 immigrations produces the following tables:
### Dimension tables
- `i94_immigrations` - A dimension table that contains details about the immigration checkpoint.
- `i94_port_state_mapping` - A table mapping the port id to its respective location.
- `i94_travel_mode` - A table capturing the various modes of travel.





## Credits
Docker airflow
https://github.com/puckel/docker-airflow
