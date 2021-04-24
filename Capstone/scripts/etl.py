"""
This is the main script which will handle the ETL pipeline
for parsing the Immigrations data including any miscellanous
datasets

Airflow will be used for orchestration
"""
import pandas as pd 
import numpy as np
import os 
import sys
import time
import pathlib
from datetime import datetime, timedelta
import configparser
import json
from functools import reduce
import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import date_add
from pyspark.sql.types import (StructType as R,
                               StructField as Fld, DoubleType as Dbl, StringType as Str,
                               IntegerType as Int, DateType as Date, TimestampType as TimeStamp
                              )

DATE_FMT = datetime.strftime(datetime.today(), '%Y%m%d')
FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
CFG_FILE = r'/Users/home/Documents/dend/Data-Engineering-ND/Capstone/config/etl_config.cfg'

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def create_spark_session():
    """
    Build a Pyspark session
    :Returns - A Pyspark object
    """
    try:
        spark = (
                    SparkSession.builder
                                .config("spark.jars.packages", "saurfang:spark-sas7bdat:3.0.0-s_2.12")
                                .enableHiveSupport()
                                .getOrCreate()
        )
    except Exception as e:
        logger.error('Pyspark session failed to be created...')
        raise
    return spark

def concat_df(*dfs):
    """
    Concat the DataFrames
    :params *dfs - A list of DataFrames()
    : Returns - A Concatenated Pyspark DataFrame
    """
    try:
        return reduce(DataFrame.unionAll, dfs)
    except Exception as e:
        logger.error('Failed to concatenate DataFrames...')
        logger.error(e)
        raise

def read_raw_files(path):
    """
    Read the raw SAS files stored locally
    and append them altogether
    :params path - Location of the raw files
    : Returns - A Concatenated Pyspark DataFrame
    """
    dfs = []
    try:
        for file in os.listdir(path):
            dfs.append(
                        spark.read.format('com.github.saurfang.sas.spark') \
                                    .load(file)
            )
    except Exception as e:
        logger.error('Failed to read raw SAS files...')
        logger.error(e)
        raise
    return concat_df(*ds)

def sas_date_converter(row, base_date='1960-01-01'):
    """
    Convert SAS specific date, which is represented
    as number of days since 1960-01-01
    :params row - A SAS date
    :params base_date - Base date used to compute SAS date today
    Returns - A Date if present otherwise a NULL is returned
    """
    if row is None:
        return row
    return datetime.strptime(base_date, '%Y-%m-%d') + timedelta(int(row))

def change_date_format(row, yrs=[2016]):
    """
    Convert the folliowing date representations: mmddyyyy and yyyymmdd
    to yyyy-mm-dd
    :params row - A Date like value or possibly a junk value
    :params yrs - A list of valid years
    Returns - A Date in %Y-%m-%d format
    """
    if row is None:
        return None
    yr = row[:-4]
    if yr in yrs:
        if row.endswith(yr):
            return datetime.strptime(row, '%m%d%Y')
        if row.startswith(yr):
            return datetime.strptime(row, '%Y%m%d')
        return None
    return None

def i94_immigrations(df, f, cols):
    """
    Build the i94_immigrations DataFrame which is one of the dimensions
    :params df - A Pyspark raw DataFrame
    :params f - A UDF
    :params cols - A list of columns
    Returns - A cleaned dimensional DataFrame
    """
    try:
        immigrations = (
            df.select(cols)
                .dropDuplicates()
                .withColumn('custom_client_id', df['cicid'].cast(Int()).cast(Str())).drop('cicid')
                .withColumn('i94_year', df['i94yr'].cast(Int())).drop('i94yr')
                .withColumn('i94_month', df['i94mon'].cast(Int())).drop('i94mon')
                .withColumnRenamed('i94port', 'i94_port')
                .withColumn('mode_of_entry', df['i94mode'].cast(Int())).drop('i94mode')
                .withColumnRenamed('visapost', 'visa_post')
                .withColumnRenamed('entdepa', 'arrival_flag')
                .withColumnRenamed('entdepd', 'depature_flag')
                .withColumnRenamed('entdepu', 'update_flag')
                .withColumnRenamed('matflag', 'match_flag')
        #        .withColumn('i94_entry_date', F.to_date('dtadfile', 'yyyymmdd').cast(Date()))
                .withColumn('i94_entry_date', f('dtadfile'))
                .drop(F.col('dtadfile'))
                .withColumn('i94_valid_till', f('dtaddto'))
                .drop(F.col('dtaddto'))
        )
    except Exception as e:
        logger.error('Failed to create i94_immigrations DataFrame...')
        logger.error(e)
        raise
    return immigrations

def i94_visitors(spark, df):
    """
    Build the i94_visitors DataFrame which is a fact
    :params spark - A Pyspark object
    :params df - A Pyspark raw DataFrame
    Returns - A cleaned fact DataFrame
    """
    try:
        df.createOrReplaceTempView('i94_visitors')
        visitors = spark.sql("""
            SELECT
                DISTINCT
                STRING(INT(cicid)) AS custom_client_id,
                STRING(INT(admnum)) AS admissions_number,
                INT(i94yr) AS i94_year,
                INT(i94mon) AS i94_month,
                STRING(INT(i94cit)) AS arrived_country_id,
                STRING(INT(i94res)) AS resident_country_id,
                SASDateConverter(arrdate) AS arrival_date,
                SASDateConverter(depdate) AS depature_date,
                STRING(fltno) AS flight_id,
                STRING(INT(i94visa)) AS visa_id,
                STRING(visatype) AS visa_category
            FROM
                i94_visitors
        """) 
    except Exception as e:
        logger.error('Failed to create i94_visitors DataFrame...')
        logger.error(e)
        raise
    return visitors

def i94_flights(spark, df):
    """
    Build the i94_flights DataFrame which is another dimension
    :params spark - A Pyspark object
    :params df - A Pyspark raw DataFrame
    Returns - A cleaned dimensional DataFrame
    """
    try:
        df.createOrReplaceTempView('i94_flights')
        flights = spark.sql("""
            SELECT
                DISTINCT
                STRING(fltno) AS flight_id,
                airline
            FROM
                i94_flights
            WHERE 
                fltno IS NOT NULL
        """)
    except Exception as e:
        logger.error('Failed to write i94_flights DataFrame into Parquet format...')
        logger.error(e)
        raise
    return flights

def i94_airports(spark, df):
    """
    Build the i94_airports DataFrame which is another dimension
    :params spark - A Pyspark object
    :params df - A Pyspark raw DataFrame
    Returns - A cleaned dimensional DataFrame
    """
    df.createOrReplaceTempView('i94_airports')
    airports = spark.sql("""
        SELECT
            DISTINCT
            STRING(ident) AS airport_id,
            type AS airport_type,
            name AS airpot_name,
            elevation_ft,
            continent,
            iso_country,
            iso_region,
            CASE WHEN iso_region LIKE 'US-%' THEN SPLIT(iso_region, '-')[1] ELSE NULL END AS us_cities,
            municipality,
            gps_code,
            iata_code,
            local_code,
            CAST(SPLIT(coordinates, ',')[0] AS DOUBLE) AS latitude,
            CAST(SPLIT(coordinates, ',')[1] AS DOUBLE) AS longitude
        FROM
            i94_airports
    """)
    return airports

def i94_us_states_demographic(spark, df):
    """
    Build the i94_us_states_demographic DataFrame which is another dimension
    :params spark - A Pyspark object
    :params df - A Pyspark raw DataFrame
    Returns - A cleaned dimensional DataFrame
    """
    df.createOrReplaceTempView('i94_us_states_demographics')
    states = spark.sql("""
    SELECT
        DISTINCT
        State AS state,
        DOUBLE(`Median Age`) AS median_age,
        INT(`Male Population`) AS male_population,
        INT(`Female Population`) AS female_population,
        INT(`Number of Veterans`) AS num_veterans,
        INT(`Foreign-born`) AS num_foreign_born,
        DOUBLE(`Average Household Size`) AS avg_household_size,
        `State Code` AS state_code
    FROM
        i94_us_states_demographics
    """)
    return states

def i94_us_cities_demographic(spark, df):
    """
    Build the i94_us_cities_demographic DataFrame which is another dimension
    :params spark - A Pyspark object
    :params df - A Pyspark raw DataFrame
    Returns - A cleaned dimensional DataFrame
    """
    df.createOrReplaceTempView('i94_us_cities_demographics')
    cities = spark.sql("""
        SELECT
            UPPER(City) AS city,
            Race AS race,
            INT(Count) AS population
        FROM
            i94_us_cities_demographics
    """)
    return cities

def i94_dates(spark, df):
    """
    Build the i94_dates DataFrame which is another dimension; this is
    built from the arrival_date field in the immigrations dataset
    :params spark - A Pyspark object
    :params df - A Pyspark cleaned DataFrame
    Returns - A cleaned dimensional DataFrame
    """
    i94_dates = (
        df.select(
                    F.col('i94_year'),
                    F.col('i94_month'),
                    F.col('arrival_date'), 
                    F.year('arrival_date').alias('year'),
                    F.month('arrival_date').alias('month'),
                    F.dayofmonth('arrival_date').alias('day'),
                    F.dayofweek('arrival_date').alias('dayofweek'),
                    F.when((F.dayofweek('arrival_date') == 1) | 
                           (F.dayofweek('arrival_date') == 7), 'weekend').otherwise('weekday').alias('is_weekend')
        )
    )
    return i94_dates

def parse_ref_file(file, start_pos=2, end_pos=7):
    """
    Parse the immigrations Data dictionary file
    The file has an unusal structure
    :params file - The file to parse
    :params start_pos - Starting position of the section to read from
    :params end_pos - Ending position of the section
    Returns - A dictionary of key, value pairs
    """
    data = []
    ref_dict = {}
    with open(file) as f:
        content = f.read()
        data = [word for lines in content.split(';')[start_pos: end_pos] for word in lines.splitlines(True) if '=' in word]
        for item in data:
            k = item.split('=')[0].strip().strip("'")
            v = item.split('=')[1].strip().strip("'") 
            if k not in ref_dict:
                ref_dict[k] = v
    return ref_dict

def write_dataframes(df, output_file, 
                    fmt='parquet', is_partition=True, is_overwrite=True):
    """
    Write the DataFrame to an output file based on the format
    :params df - A Pyspark DataFrame
        :params output_file - A filename
    :params fmt - Type of output file format
    :params is_partition - A boolean to indicate whether to partition or not
    :params is_overwrite - A boolean to indicate whether to overwrite or not
    Returns - None
    """
    try:
        if fmt == 'parquet':
            if is_partition:
                if is_overwrite:
                    (
                        df.write.mode('overwrite')
                                    .partitionBy('i94_year', 'i94_month')
                                    .parquet(output_file)
                    )
                else:
                    (
                        df.write.mode('append')
                                    .partitionBy('i94_year', 'i94_month')
                                    .parquet(output_file)
                    )
            else:
                if is_overwrite:
                    (
                        df.write.mode('overwrite')
                                    .parquet(output_file)
                    )
                else:
                    (
                        df.write.mode('append')
                                    .parquet(output_file)
                    )
        else:
            if is_partition:
                if is_overwrite:
                    (
                        df.write.mode('overwrite')
                                    .partitionBy('i94_year', 'i94_month')
                                    .csv(output_file)
                    )
                else:
                    (
                        df.write.mode('append')
                                    .partitionBy('i94_year', 'i94_month')
                                    .csv(output_file)
                    )
            else:
                if is_overwrite:
                    (
                        df.write.mode('overwrite')
                                    .csv(output_file)
                    )
                else:
                    (
                        df.write.mode('append')
                                    .csv(output_file)
                    )

    except Exception as e:
        logger.error(f'Failed to write {output_file} DataFrame into {fmt} format...')
        logger.error(e)
        raise


def enable_logging(log_dir, log_file):
    """
    Enable logging across modules
    :params log_dir - Location of the log directory
    :params log_file - Base file name for the log
    Returns - A FileHandler object
    """
    # instantiate logging
    file_handler = logging.FileHandler(os.path.join(log_dir, log_file + DATE_FMT))
    formatter = logging.Formatter(FORMAT)
    file_handler.setFormatter(formatter)

    return file_handler

def main():
    """
    - Create a Pyspark object
    - Read the SAS files
    - Create the dimensional and fact DataFrames
    - Write them into partitioned/non-partitioned Parquet/CSV formats
    """
    t0 = time.time()
    config = configparser.ConfigParser()

    try:
        config.read(CFG_FILE)
    except Exception as e:
        print('Configuration file is missing or cannot be read...')
        raise

    base_dir = config['LOCAL']['base_dir']
    log_dir = os.path.join(base_dir, config['LOCAL']['log_dir'])
    log_file = config['LOCAL']['log_file']
    print("Create log dir if it doesn't exist...")
    pathlib.Path(log_dir).mkdir(exist_ok=True)
    file_handler = enable_logging(log_dir, log_file)
    logger.addHandler(file_handler)

    logger.info('ETL parsing has started...')
    spark = create_spark_session()
    logger.info('Pyspark session created...')

    path = config['LOCAL']['sas_data_dir']
    files = json.loads(config['LOCAL']['input_files'])
    airport_file = os.path.join(base_dir, config['LOCAL']['airports_file'])
    demographic_file = os.path.join(base_dir, config['LOCAL']['us_demographics_file'])
    output_dir = os.path.join(base_dir, config['LOCAL']['output_dir'])
    logger.info("Create output dir if it doesn't exist...")
    pathlib.Path(output_dir).mkdir(exist_ok=True)

    logger.info('Read and concatenate the raw SAS files...')
    dfs = []
    for file in files:
        df = spark.read.format('com.github.saurfang.sas.spark')\
                    .load(os.path.join(base_dir, path, file))
        dfs.append(df)
    logger.info(f'Read {len(files)} files successfully...')
    df = concat_df(*dfs)
    logger.info(f'Successfully concatenated {len(files)}...')

    spark.udf.register('SASDateConverter', sas_date_converter, Date())
    logger.info('Register sas_date_converter UDF...')

    # change_date_format_1 = F.udf(lambda x: datetime.strptime(x.strip(), '%Y%m%d'), Date())
    # change_date_format_2 = F.udf(lambda x: datetime.strptime(x.strip(), '%m%d%Y'), Date())
    dt = F.udf(change_date_format, Date())

    table = 'i94_immigrations'
    logger.info(f'Create {table} DataFrame...')
    cols = ['cicid', 'i94yr', 'i94mon', 'i94port', 'i94mode', 'visapost', 
       'entdepa', 'entdepd', 'entdepu', 'matflag', 
       'dtadfile', 'dtaddto']
    res_df = i94_immigrations(df, dt, cols)
    logger.info(f'Successfully created {table}...')
    logger.info(f'Number of records {res_df.count()}')

    fmt = 'parquet'
    write_dataframes(res_df, os.path.join(output_dir, table), 
                    fmt=fmt, is_partition=True, is_overwrite=True)
    logger.info(f'Successfully written {table} into Parquet format...')

    table = 'i94_visitors'
    logger.info(f'Create {table} DataFrame...')
    res_visitor_df = i94_visitors(spark, df)
    logger.info(f'Successfully created {table}...')
    logger.info(f'Number of records {res_df.count()}')

    write_dataframes(res_df, os.path.join(output_dir, table),
                    fmt=fmt, is_partition=True, is_overwrite=True)
    logger.info(f'Successfully written {table} into Parquet format...')

    table = 'i94_dates'
    logger.info(f'Create {table} DataFrame...')
    res_df = i94_dates(spark, res_visitor_df)
    logger.info(f'Successfully created {table}...')
    logger.info(f'Number of records {res_df.count()}')

    write_dataframes(res_df, os.path.join(output_dir, table),
                    fmt=fmt, is_partition=True, is_overwrite=True)
    logger.info(f'Successfully written {table} into parquet format...') 

    table = 'i94_flights'
    logger.info(f'Create {table} DataFrame...')
    res_df = i94_flights(spark, df)
    logger.info(f'Successfully created {table}...')
    logger.info(f'Number of records {res_df.count()}')

    fmt = 'csv'
    write_dataframes(res_df, os.path.join(output_dir, table),
                    fmt=fmt, is_partition=False, is_overwrite=True)
    logger.info(f'Successfully written {table} into csv format...')
    

    logger.info('Read the airports reference file...')
    airport_df = spark.read.option('header', True) \
                            .csv(airport_file)
    
    table = 'i94_airports'
    logger.info(f'Create {table} DataFrame...')
    res_df = i94_airports(spark, airport_df)
    logger.info(f'Successfully created {table}...')
    logger.info(f'Number of records {res_df.count()}')

    write_dataframes(res_df, os.path.join(output_dir, table),
                    fmt=fmt, is_partition=False, is_overwrite=True)
    logger.info(f'Successfully written {table} into csv format...')

    logger.info('Read the US demographics reference file...')
    demographic_df = spark.read.options(header='True', delimiter=';') \
                            .csv(demographic_file)
    
    table = 'i94_us_states_demographic'
    logger.info(f'Create {table} DataFrame...')
    res_df = i94_us_states_demographic(spark, demographic_df)
    logger.info(f'Successfully created {table}...')
    logger.info(f'Number of records {res_df.count()}')

    write_dataframes(res_df, os.path.join(output_dir, table),
                    fmt=fmt, is_partition=False, is_overwrite=True)
    logger.info(f'Successfully written {table} into csv format...') 

    table = 'i94_us_cities_demographic'
    logger.info(f'Create {table} DataFrame...')
    res_df = i94_us_cities_demographic(spark, demographic_df)
    logger.info(f'Successfully created {table}...')
    logger.info(f'Number of records {res_df.count()}')

    write_dataframes(res_df, os.path.join(output_dir, table),
                    fmt=fmt, is_partition=False, is_overwrite=True)
    logger.info(f'Successfully written {table} into csv format...')

    logger.info('ETL parsing has completed...')
    logger.info('Time taken to complete job {} minutes'.format((time.time() - t0) / 60))

if __name__ == '__main__':
    main()

    





