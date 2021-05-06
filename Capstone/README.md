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

There are 3 main sources of data; the I94 immigrations dataset, including the dictionary produces the following tables:
### Dimension tables
- `i94_immigrations` - A dimension table that contains details about the immigration checkpoint.
- `i94_port_state_mapping` - A table mapping the port id to its respective location.
- `i94_travel_mode` - A table capturing the various modes of travel.
- `i94_dates` - A derived table using arrival_date. It captures various dimensions of a data attribute.
- `i94_visitors` - A table that shows the demographic details about the visitor.
- `i94_flights` - A table capturing the flight information.
- `i94_countries` - A table capturing countries.
- `i94_visa` - This table shows the various categories of visas offered.
- `i94_state_mapping` - A table showing U.S. State information.
### Fact table
- `i94_trips` - Captures the visits made by the individual during the year.

From the U.S. cities demograhic dataset,t he following tables were produced:
### Dimension tables
- `i94_us_states_demograhic` - Captures census count by state.
- `i94_us_cities_demographic` - Captures city-wise population count. Also include race.

Finally, the airports dataset captures:
### Dimension table
- `i94_airports` - Location and type of the airport.

# Jobs description
- The ETL script will read the datasets from the local drive and create the required tables, and store them as Parquet/CSV files into an S3 bucket in their appropriate folders.
- It copies the config file and transformed datasets for the next job.
- The Data quality check job will check for empty records and also check for null keys.
- The logs are finally copied into an S3 folder.
- The orchestration is handled in Airflow.

# How to run
This Proof of Concept was built and tested on Mac OS Catalina 10.15.5
- Installed version of Airflow - 1.10.2
- Installed version of Python - 3.6.13
- Installed version of psycopg2 - 2.8.6
- Installed version of PostgreSQL - 13.2
- Installed version of JRE - 1.8.0_281
- Installed Pyspark version - 3.1.1

- Create an environment by running the following commands on the terminal
```
  conda create --name capstone python=3.6
  pip install pyspark
  pip install apache-airflow
```
- You may need to install extras to support Amazon Web Services.
- Once Apache is successfully installed, do the following:
```
- export AIRFLOW_HOME = ~/airflow
- Create a database in postgres called airflow with airflow being both the user and password
- Go the ~/airflow and edit the following entries in the airflow.cfg file:
- dags_folder - Point it to where the dags are located
- plugins_folder - Point it to where the plugins are located
- sql_alchemy_conn - Set it to the following: postgresql+psycopg2://airflow:airflow@127.0.0.1:5432/airflow. SQLlite doesn't support running multiple dags.
- airflow dbinit - Initializes the Database
```
- Add these lines into a script to start the Airflow scheduler and webserver
```
#!/bin/bash
# Start airflow
airflow scheduler --daemon
airflow webserver --daemon -p 8080

# Wait till airflow web-server is ready
echo "Waiting for Airflow web server..."
while true; do
  _RUNNING=$(ps aux | grep airflow-webserver | grep ready | wc -l)
  if [ $_RUNNING -eq 0 ]; then
    sleep 1
  else
    echo "Airflow web server is ready"
    break;
  fi
done
```
- To stop the Airflow server, run `cat $AIRFLOW_HOME/airflow-webserver.pid | xargs kill -9`
- I have noticed that deleting a dag from UI doesn't delete it at all. So, you have to do so from the command line, as follows:
- Temporarily Move the offending dag from its location into another location
- Run `airflow delete_dag i94_run_etl`
- Move the dag back into its original location
- Run the script to start the server
- The Airflow UI can be accessed from `http://localhost:8080`
- Inside the UI, go to Admin -> Connections. Here enter the AWS credentials, namely the connection id - aws_credentials, conn_type - Amazon Web Services
login - Your aws access key id, password secret access key, and save them.

The above steps can be abstracted away if Docker is used. However, I ran into dependency issues inside Amazon EMR, and had to backtrack.

Git clone the repo `https://github.com/sl2902/Data-Engineering-ND.git`. `cd Capstone`

The directory structure should look like so:
```
Capstone
    ├── Dockerfile
    ├── README.md
    ├── airflow
    │   ├── dags
    │   │   ├── __init__.py
    │   │   ├── i94_create_and_copy_files_dag.py
    │   │   └── i94_run_etl_dag.py
    │   └── plugins
    │       ├── __init__.py
    │       └── operators
    │           ├── __init__.py
    │           ├── copy_files_to_s3.py
    │           └── create_s3_bucket.py
    ├── config
    │   ├── __init__.py
    │   ├── airflow.cfg
    │   └── etl_config.cfg
    ├── data
    │   └── 18-83510-I94-Data-2016
    │       ├── i94_apr16_sub.sas7bdat
    │       ├── i94_aug16_sub.sas7bdat
    │       ├── i94_dec16_sub.sas7bdat
    │       ├── i94_feb16_sub.sas7bdat
    │       ├── i94_jan16_sub.sas7bdat
    │       ├── i94_jul16_sub.sas7bdat
    │       ├── i94_jun16_sub.sas7bdat
    │       ├── i94_mar16_sub.sas7bdat
    │       ├── i94_may16_sub.sas7bdat
    │       ├── i94_nov16_sub.sas7bdat
    │       ├── i94_oct16_sub.sas7bdat
    │       └── i94_sep16_sub.sas7bdat
    ├── dictionary
    │   └── I94_SAS_Labels_Descriptions.SAS
    ├── docker-compose-CeleryExecutor.yml
    ├── notebook
    │   ├── Capstone\ Project\ Template.ipynb
    │   ├── Explore_using_PySpark.ipynb
    │   └── misc_function_testing.ipynb
    ├── requirements.txt
    ├── script
    │   └── entrypoint.sh
    └── scripts
        ├── etl.py
        ├── i94_data_quality_check.py
```

The files and folders of interest are:
- airflow - contains the dags and plugins
- config - contains the configuration script
- data - contains the SAS datasets including the CSV files
- dictionary - contains the data dictionary
- scripts - contains the ETL pipeline scripts

Edit the following entries in the `etl_config.cfg` script:
- Under section S3 - s3_bucket
- Under section LOCAL - input_files, which is a list. Here you can list out the files you want processed.
- Under section APP - sas_jar_ver
- Under section DQ - tables and table_col

Start the Airflow server. From the Airflow UI, toggle the switch from OFF to ON, and refresh your page. You should see the dag start.
![image](https://user-images.githubusercontent.com/7212518/117315961-c9002980-aea5-11eb-9bdf-30d259ca829f.png)

The scripts can be tested locally as well, like so
Run
```
python etl.py
python i94_data_quality_check.py --tables='["i94_visa", "i94_travel_mode"]' --table-col='{"i94_visa": ["visa_id"], "i94_travel_mode": ["mode_id"]}'
```








## Credits
Docker airflow
https://github.com/puckel/docker-airflow
