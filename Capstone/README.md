# Data-Engineering-ND
Ever since the dawn of human civilization, man has been moving from place to place in search of food, water, shelter. etc. In the modern world, country borders, for most parts, are clearly demarcated, which means movement of men and material is not as simple as walking across the border; this is where the more formal term - Immigrations, comes in. It is the act of coming to live in another person's country for various reasons. These checkpoints could be at airports, seaports or land borders. Here government officials check documents to ensure the traveler is carrying all necessary legal documents for his/her stay in the country.

The data provided for this project is from the US I94 Immigrations 2016 time period. According to [Wikipedia](https://en.wikipedia.org/wiki/Immigration_to_the_United_States#:~:text=According%20to%20the%202016%20Yearbook,565k%20status%20adjustments) - the 2016 Yearbook of Immigration Statistics, the United States admitted a total of 1.18 million legal immigrants. The consequences are plenty: from adding to the population to adding to the skilled workforce, and from increasing crime rate to burdening social securiy.

The analysis of such data along with additional sources such as the US census demographic data can help Immigrations officials better deal with the influx of visitors thronging their country every day.

# Data sources
## 1. U.S. I94 immigrations data 2016
This is the main data source, and it has 12 files - one for each month of the year; the files are stored in sas7bdat format, and each file has roughly 3-4 million records for a total size of ~6GB. The data comes from here - th, e [US National Tourism and Trade Office](https://travel.trade.gov/research/reports/i94/historical/2016.html). A data dictionary is also provided to supplement these data.

## 2. U.S. Cities demographics 2015
This dataset contains information about the demographics of all US cities and census-designated places with a population greater or equal to 65,000. 
This data comes from the US Census Bureau's 2015 American Community Survey. Here is the source [OpendDataSoft](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/information/)

## 3. Airport codes
This is a simple table of airport codes and corresponding cities. It comes from (DataHub)[https://datahub.io/core/airport-codes#data]

## Credits
Docker airflow
https://github.com/puckel/docker-airflow
