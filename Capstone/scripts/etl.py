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
# CFG_FILE = r'/usr/local/airflow/config/etl_config.cfg'
CFG_FILE = "s3://immigrations-analytics/config/etl_config.cfg"

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

def i94_trips(spark, df):
    """
    Build the i94_trips DataFrame which is a fact
    :params spark - A Pyspark object
    :params df - A Pyspark raw DataFrame
    Returns - A cleaned fact DataFrame
    """
    try:
        df.createOrReplaceTempView('i94_trips')
        trips = spark.sql("""
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
                i94_trips
        """) 
    except Exception as e:
        logger.error('Failed to create i94_trips DataFrame...')
        logger.error(e)
        raise
    return trips

def i94_visitors(spark, df):
    """
    Build the i94_visitors DataFrame which is a dimension
    :params spark - A Pyspark object
    :params df - A Pyspark raw DataFrame
    Returns - A cleaned fact DataFrame
    """
    try:
        df.createOrReplaceTempView('i94_visitors')
        visitors = spark.sql("""
            SELECT
                DISTINCT
                STRING(INT(admnum)) AS admissions_number,
                INT(i94yr) AS i94_year,
                INT(i94mon) AS i94_month,
                STRING(INT(i94res)) AS resident_country_id,
                INT(biryear) AS birth_year,
                gender
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
        ).dropDuplicates()
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

def build_df_from_ref_file(ref_file, start_pos=2, end_pos=7, 
                            col_name=None, index_name=None):
    """
    Create DataFrame from the parsed Data dictionary file
    :params ref_file - SAS Data dictionary file
    :params start_pos - Starting position of the section to read from
    :params end_pos - Ending position of the section
    :params col_name - Name of the new column
    :params index_name - Name of the new index
    Returns - A DataFrame
    """
    return (
        pd.Series(parse_ref_file(ref_file, start_pos, end_pos))
        .to_frame()
        .rename(columns={0: col_name})
        .reset_index()
        .rename(columns={'index': index_name})
    )

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
                        df.write.option('header', 'True')
                                    .mode('overwrite')
                                    .partitionBy('i94_year', 'i94_month')
                                    .parquet(output_file)
                    )
                else:
                    (
                        df.write.option('header', 'True')
                                    .mode('append')
                                    .partitionBy('i94_year', 'i94_month')
                                    .parquet(output_file)
                    )
            else:
                if is_overwrite:
                    (
                        df.write.option('header', 'True')
                                    .mode('overwrite')
                                    .parquet(output_file)
                    )
                else:
                    (
                        df.write.option('header', 'True')
                                    .mode('append')
                                    .parquet(output_file)
                    )
        else:
            if is_partition:
                if is_overwrite:
                    (
                        df.write.option('header', 'True')
                                    .mode('overwrite')
                                    .partitionBy('i94_year', 'i94_month')
                                    .csv(output_file)
                    )
                else:
                    (
                        df.write.option('header', 'True')
                                    .mode('append')
                                    .partitionBy('i94_year', 'i94_month')
                                    .csv(output_file)
                    )
            else:
                if is_overwrite:
                    (
                        df.write.option('header', 'True')
                                .mode('overwrite')
                                .csv(output_file)
                    )
                else:
                    (
                        df.write.option('header', 'True')
                                    .mode('append')
                                    .csv(output_file)
                    )

    except Exception as e:
        logger.error(f'Failed to write {output_file} DataFrame into {fmt} format...')
        logger.error(e)
        raise

def create_and_write_df(df, table, f_transform, 
                        output_dir,
                        spark=None, cols=None,
                        udf=None, fmt='parquet',
                        is_partition=True,
                        is_overwrite=True,
                        crate_date_df=False):
    """
    Helper function to perform both DataFrame creation and
    writing the results to either Parquet or CSV
    :params df - A Pyspark/Pandas DataFrame
    :params table - Name of the transformed file
    :params f_transform - Function to apply
    :params output_dir - Output directory to write file to
    :params spark - A Pyspark object
    :params cols - A list of columns to select
    :params udf - Pyspark UDF
    :params fmt - Format type to apply
    :params is_partition - A boolean to decide whether to parition or not
    :params is_overwrite - A boolean to decide whether to overwrite or append
    :params create_date_df - Boolean to determine whether to
                            create DataFrame for Date dimension
    Returns - A Pyspark Dataframe if create_date_df=True
    """
    res_df = None
    if spark is None:
        res_df = f_transform(df, udf, cols)
    else:
        res_df = f_transform(spark, df)
    logger.info(f'Successfully created {table}...')
    logger.info(f'Number of records {res_df.count()}')

    write_dataframes(res_df, os.path.join(output_dir, table), 
                    fmt=fmt, is_partition=is_partition, is_overwrite=is_overwrite)
    logger.info(f'Successfully written {table} into {fmt.capitalize()} format...')

    return res_df

def create_and_write_ref_df(dictionary_file, table, output_dir, spark, 
                        fmt='csv', start_pos=2, end_pos=3,
                        col_name=None, index_name=None,
                        is_partition=True,
                        is_overwrite=True):
    """
    Helper function to create and write reference dimensions into CSV
    :params dictionary_file - The SAS data dictionary
    :params table - Name of the transformed file
    :params fmt - Format type to apply
    :params start_pos - Starting position of the section to read from
    :params end_pos - Ending position of the section
    :params col_name - Name of the new column
    :params index_name - Name of the new index
    :params is_partition - A boolean to decide whether to parition or not
    :params is_overwrite - A boolean to decide whether to overwrite or append
    Returns - None
    """
    res_df = build_df_from_ref_file(dictionary_file, start_pos=start_pos, end_pos=end_pos, 
                            col_name=col_name, index_name=index_name) 
    logger.info(f'Successfully created {table}...')
    logger.info(f'Number of records {len(res_df)}')
    if table == 'i94_port_state_mapping':
            logger.info(f'Updating the column names for {table}...')
            res_df = pd.concat([res_df['i94_port'].to_frame(), res_df['city'].str.strip().str.rsplit(',', 1, expand=True)], axis=1)
            res_df.rename(columns={0: 'city', 1: 'state'}, inplace=True)

    write_dataframes(spark.createDataFrame(res_df), os.path.join(output_dir, table),
                    fmt=fmt, is_partition=is_partition, is_overwrite=is_overwrite)
    logger.info(f'Successfully written {table} into csv format...')


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

    # base_dir = config['LOCAL']['base_dir']
    # log_dir = os.path.join(base_dir, config['LOCAL']['log_dir'])
    # log_file = config['LOCAL']['log_file']
    base_dir = config['HADOOP']['base_dir']
    log_dir = os.path.join(base_dir, config['HADOOP']['log_dir'])
    log_file = config['HADOOP']['log_file']
    print("Create log dir if it doesn't exist...")
    pathlib.Path(log_dir).mkdir(exist_ok=True)
    file_handler = enable_logging(log_dir, log_file)
    logger.addHandler(file_handler)

    logger.info('ETL parsing has started...')
    spark = create_spark_session()
    logger.info('Pyspark session created...')

    # data_dir = config['LOCAL']['data_dir']
    # path = config['LOCAL']['sas_data_dir']
    # sas_file_path = os.path.join(base_dir, data_dir, path)
    # dict_dir = config['LOCAL']['dict_dir']
    # files = json.loads(config['LOCAL']['input_files'])
    # airport_file = os.path.join(base_dir, data_dir, config['LOCAL']['airports_file'])
    # demographic_file = os.path.join(base_dir, data_dir, config['LOCAL']['us_demographics_file'])
    # dictionary_file = os.path.join(base_dir, dict_dir, config['LOCAL']['dictionary_file'])
    # output_dir = os.path.join(base_dir, config['LOCAL']['output_dir'])
    data_dir = config['S3']['s3_bucket']
    path = config['S3']['s3_sas_key']
    sas_file_path = os.path.join("s3://", data_dir, path)
    dict_dir = config['LOCAL']['s3_dict_key']
    csv_dir = config['LOCAL']['s3_csv_key']
    files = json.loads(config['LOCAL']['input_files'])
    airport_file = os.path.join("s3://", data_dir, csv_dir, config['S3']['airport_file'])
    demographic_file = os.path.join("s3://", data_dir, csv_dir, config['S3']['demographic_file'])
    dictionary_file = os.path.join("s3://", dict_dir, config['S3']['dictionary_file'])
    output_dir = os.path.join("s3://", data_dir, config['s3']['output_dir'])

    logger.info("Create output dir if it doesn't exist...")
    pathlib.Path(output_dir).mkdir(exist_ok=True)

    logger.info('Read and concatenate the raw SAS files...')
    dfs = []
    for file in files:
        df = spark.read.format('com.github.saurfang.sas.spark')\
                    .load(os.path.join(sas_file_path, file))
        dfs.append(df)
    logger.info(f'Read {len(files)} files successfully...')
    df = concat_df(*dfs)
    logger.info(f'Successfully concatenated {len(files)}...')

    spark.udf.register('SASDateConverter', sas_date_converter, Date())
    logger.info('Register sas_date_converter UDF...')

    # change_date_format_1 = F.udf(lambda x: datetime.strptime(x.strip(), '%Y%m%d'), Date())
    # change_date_format_2 = F.udf(lambda x: datetime.strptime(x.strip(), '%m%d%Y'), Date())
    dt = F.udf(change_date_format, Date())

    # SAS raw data table creation begins here
    cols = ['cicid', 'i94yr', 'i94mon', 'i94port', 'i94mode', 'visapost', 
       'entdepa', 'entdepd', 'entdepu', 'matflag', 
       'dtadfile', 'dtaddto']
    parquet_tables = ['i94_immigrations', 'i94_trips', 'i94_visitors', 'i94_flights']
    f_transforms = [i94_immigrations, i94_trips, i94_visitors, i94_flights]
    res_df = None
    for table, f_transform in zip(parquet_tables, f_transforms):
        if table == 'i94_immigrations':
            res_df = create_and_write_df(df, table, f_transform, 
                            output_dir,
                            spark=None, cols=cols,
                            udf=dt, fmt='parquet',
                            is_partition=True,
                            is_overwrite=True,
                            crate_date_df=False)
        elif table == 'i94_flights':
            res_df = create_and_write_df(df, table, f_transform, 
                output_dir,
                spark=spark, cols=None,
                udf=None, fmt='csv',
                is_partition=False,
                is_overwrite=True,
                crate_date_df=False)
        else:
            res_df = create_and_write_df(df, table, f_transform, 
                        output_dir,
                        spark=spark, cols=None,
                        udf=None, fmt='parquet',
                        is_partition=True,
                        is_overwrite=True,
                        crate_date_df=False)

        if table == 'i94_trips':
            table = 'i94_dates'
            create_and_write_df(res_df, table, i94_dates, 
                        output_dir,
                        spark=spark, cols=None,
                        udf=None, fmt='parquet',
                        is_partition=True,
                        is_overwrite=True,
                        crate_date_df=False)

    # Reference data for airports and us city demographics begins here
    logger.info('Read the airports reference file...')
    airport_df = spark.read.option('header', True) \
                            .csv(airport_file)

    logger.info('Read the US demographics reference file...')
    demographic_df = spark.read.options(header='True', delimiter=';') \
                            .csv(demographic_file)                        
    csv_tables = ['i94_airports', 'i94_us_states_demographic', 
            'i94_us_cities_demographic']
    f_transforms = [i94_airports, i94_us_states_demographic, i94_us_cities_demographic]
    csv_dfs = [airport_df, demographic_df, demographic_df]
    for table, f_transform, df in zip(csv_tables, f_transforms, csv_dfs):
        res_df = create_and_write_df(df, table, f_transform, 
                        output_dir,
                        spark=spark, cols=None,
                        udf=dt, fmt='csv',
                        is_partition=False,
                        is_overwrite=True)
    
    # SAS reference data creation begins here
    ref_csv_tables = ['i94_countries', 'i94_port_state_mapping', 'i94_travel_mode', 
            'i94_state_mapping', 'i94_visa']
    table_pos_dict = {
        'i94_countries': [2, 3, 'country', 'country_id'],
        'i94_port_state_mapping': [3, 4, 'city', 'i94_port'],
        'i94_travel_mode': [4, 5, 'mode', 'mode_id'],
        'i94_state_mapping': [5, 6, 'state', 'state_id'],
        'i94_visa': [6, 7, 'visa_purpose', 'visa_id']
    }
    logger.info('Read the SAS data dictionary reference file...') 
    for table in ref_csv_tables:
        create_and_write_ref_df(dictionary_file, table, output_dir, spark, 
                        fmt='csv', start_pos=table_pos_dict[table][0], 
                        end_pos=table_pos_dict[table][1],
                        col_name=table_pos_dict[table][2], 
                        index_name=table_pos_dict[table][3],
                        is_partition=False,
                        is_overwrite=True)

    logger.info('ETL parsing has completed...')
    logger.info('Time taken to complete job {} minutes'.format((time.time() - t0) / 60))

if __name__ == '__main__':
    main()

    





