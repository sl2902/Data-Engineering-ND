"""
This scripts performs some basic data quality checks
to ensure that the transformed data was correctly loaded

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
import boto3
import argparse
from pyspark import SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import date_add
from pyspark.sql.types import (StructType as R,
                               StructField as Fld, DoubleType as Dbl, StringType as Str,
                               IntegerType as Int, DateType as Date, TimestampType as TimeStamp
                              )

DATE_FMT = datetime.strftime(datetime.today(), '%Y%m%d')
FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
# CFG_FILE = r'/usr/local/airflow/config/etl_config.cfg'
# CFG_FILE = "s3://immigrations-analytics1/config/etl_config.cfg"
# SAS_JAR = 'saurfang:spark-sas7bdat:3.0.0-s_2.12'
# SAS_JAR = "saurfang:spark-sas7bdat:2.0.0-s_2.11"

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def create_client(service, region, access_key_id, secret_access_key):
    """
    Create client to access AWS resource
    :params service - Any AWS service
    :params region - AWS specific region
    :params access_key_id - AWS credential
    :params secret_access_key - AWS credential
    Returns - A boto3 client
    """
    client = boto3.client(service,
                          region_name=region,
                          aws_access_key_id=access_key_id,
                          aws_secret_access_key=secret_access_key
                          )
    return client

def create_spark_session():
    """
    Build a Pyspark session
    Returns - A Pyspark object
    """
    try:
        spark = (
                    SparkSession.builder
                                # .config("spark.hadoop.fs.s3a.awsAccessKeyId", os.environ['AWS_ACCESS_KEY_ID'])
                                # .config("spark.hadoop.fs.s3a.awsSecretAccessKey", os.environ['AWS_SECRET_ACCESS_KEY'])
                                .enableHiveSupport()
                                .getOrCreate()
        )
        # spark._jsc.hadoopConfiguration().set("fs.s3a.awsAccessKeyId", os.environ['AWS_ACCESS_KEY_ID'])
        # spark._jsc.hadoopConfiguration().set("fs.s3a.awsSecretAccessKey", os.environ['AWS_SECRET_ACCESS_KEY'])
        # spark._jsc.hadoopConfiguration().set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
        # spark._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
        # spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider")
        # spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")
    except Exception as e:
        logger.error('Pyspark session failed to be created...')
        raise
    return spark

def check_empty_table(spark, df):
    """
    Checks whether the given table is empty or not. It should
    have at least 1 row
    :params spark - A Pyspark object
    :params df - A Pyspark DataFrame
    Returns - Number of records
    """
    return df.count()

def check_null_columns(spark, df, cols):
    """
    Checks whether primary key columns have null value in them
    :params spark - A Pyspark object
    :params df - A Pyspark DataFrame
    :params cols - A list of columns
    Returns - A list of columns with null values
    """
    null_cols_list = []
    # this assumes the col list matches the original schema
    # df = df.toDF(*cols)
    try:
        df = df.select(*cols)
        col_null_count = df.select([F.count(F.when(F.isnan(col) | F.col(col).isNull(), col)).alias(col) for col in cols]).toPandas().to_dict()
        null_cols_list = [k for k, v in col_null_count.items() if v[0] > 0]
    except Exception as e:
        logger.error('Probably and invalid column(s) was passed...')
        return ['failed']
    return null_cols_list

def enable_logging(log_dir, log_file):
    """
    Enable logging across modules
    :params log_dir - Location of the log directory
    :params log_file - Base file name for the log
    Returns - A FileHandler object
    """
    # instantiate logging
    file_handler = logging.FileHandler(os.path.join(log_dir, log_file + DATE_FMT + '.log'))
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
    parser = argparse.ArgumentParser()
    parser.add_argument('-e', '--env', default='LOCAL', help='Enter one of DOCKER, LOCAL or S3')
    parser.add_argument('--bucket-name', help='Enter S3 bucket')
    parser.add_argument('--aws-access-key-id', help='Enter AWS access key id')
    parser.add_argument('--aws-secret-access-key', help='Enter AWS secrest access key')
    parser.add_argument('--aws-region', default='us-west-2', help='Enter AWS region')
    parser.add_argument('--tables', default='[]', type=json.loads, help='Enter list of tables to check')
    parser.add_argument('--table-col', default='{}', type=json.loads, help='Enter list of tables to check')
    # subparser = parser.add_subparsers(dest='subcommand', help='Can choose bucket name if S3 is chosen')
    # parser_bucket = subparser.add_parser('S3')
    # parser_bucket.add_argument('bucket', help='S3 bucket name')
    args = vars(parser.parse_args())
    args['env'] = args['env'].upper()
    if args['env'] != 'S3' and args['bucket_name']:
        parser.error('Can specify a bucket name with only S3...')
    if args['env'] == 'S3' and not (args['bucket_name'] and 
                                args['aws_access_key_id'] and
                                args['aws_secret_access_key']):
        parser.error('Specify a bucket, access key and secret access key...')
        raise
    # print(args)
    # print(args['env'])
    # print(args['subcommand'])


    if args['env'] == 'S3':
        s3_client = create_client(
                        "s3",
                        region=args['aws_region'],
                        access_key_id=args['aws_access_key_id'],
                        secret_access_key=args['aws_secret_access_key']
                    )
        os.environ['AWS_ACCESS_KEY_ID'] = args['aws_access_key_id'].strip()
        os.environ['AWS_SECRET_ACCESS_KEY'] = args['aws_secret_access_key'].strip()


    tables = args['tables']
    table_col_dict = args['table_col']

    config = configparser.ConfigParser()
    if args['env'] == 'DOCKER':
        CFG_FILE = r'/usr/local/airflow/config/etl_config.cfg'
        try:
            config.read(CFG_FILE)
        except Exception as e:
            print('Configuration file is missing or cannot be read...')
            raise
    elif args['env'] == 'S3':
        obj = s3_client.get_object(Bucket=args['bucket_name'], Key='config/etl_config.cfg')
        try:
            config.read_string(obj['Body'].read().decode())
        except Exception as e:
            print('Configuration file is missing or cannot be read...')
            raise
    else:
        CFG_FILE = r'/Users/home/Documents/dend/Data-Engineering-ND/Capstone/config/etl_config.cfg'
        try:
            config.read(CFG_FILE)
        except Exception as e:
            print('Configuration file is missing or cannot be read...')
            raise


    if args['env'] == 'DOCKER':
        base_dir = config['DOCKER']['base_dir']
        log_dir = os.path.join(base_dir, config['LOCAL']['log_dir'])
        log_file = config['LOCAL']['dq_log_file']
        output_dir = os.path.join(base_dir, config['DOCKER']['output_dir'])
    elif args['env'] == 'S3':
        bucket = args['bucket_name']
        output_dir = config['S3']['s3_output_key']
        output_dir = os.path.join("s3a//", bucket, output_dir)
    else:
        base_dir = config['LOCAL']['base_dir']
        # log_dir = os.path.join(base_dir, config['LOCAL']['log_dir'])
        # log_file = config['LOCAL']['log_file']
        output_dir = os.path.join(base_dir, config['LOCAL']['output_dir'])
    
    try:
        # Log file written to Hadoop EMR env
        base_dir = config['HADOOP']['base_dir']
        log_dir = os.path.join(base_dir, config['HADOOP']['log_dir'])
        log_file = config['HADOOP']['dq_log_file']
        pathlib.Path(log_dir).mkdir(exist_ok=True)
        file_handler = enable_logging(log_dir, log_file)
        logger.addHandler(file_handler)
        print("Create log dir if it doesn't exist...")
    except:
        base_dir = config['LOCAL']['base_dir']
        log_dir = os.path.join(base_dir, config['LOCAL']['log_dir'])
        log_file = config['LOCAL']['dq_log_file']
        pathlib.Path(log_dir).mkdir(exist_ok=True)
        file_handler = enable_logging(log_dir, log_file)
        logger.addHandler(file_handler)
        print("Create log dir if it doesn't exist...")


    logger.info('Data quality check has started...')
    spark = create_spark_session()
    logger.info('Pyspark session created...')
    logger.info("Check whether table exists...")
    valid_tables = []
    if args['env'] == 'S3':
        for table in tables:
            res = s3_client.list_objects(Bucket=bucket, Prefix=os.path.join(output_dir, table))
            if 'Contents' in res:
                valid_tables.append(table)
            else:
                logger.error(f'Table {table} is invalid...')
    else:
        for table in tables:
            try:
                if os.path.isdir(os.path.join(output_dir, table)):
                    valid_tables.append(table)
            except Exception as e:
                logger.error(f'Table {table} is invalid...')
                logger.error(e)
    # assume the table names are the same in the
    # list and dict
    if len(table_col_dict) > 0:
        valid_table_cols = {table: table_col_dict[table] for table in valid_tables}
    else:
        valid_table_cols = {}

    logger.info('Checking for empty Dataframes...')
    if len(valid_tables) > 0:
        for table in tables:
            try:
                df = spark.read.parquet(os.path.join(output_dir, table), header=True)
                logger.info(f'Table {table} being checked is a parquet table')
            except:
                df = spark.read.csv(os.path.join(output_dir, table), header=True)
                logger.info(f'Table {table} being checked is a csv table...')
            if check_empty_table(spark, df) == 0:
                logger.error(f'Table {table} has empty rows...')
            else:
                logger.info(f'Table {table} has at least 1 record...')
    else:
        logger.info('No tables to check...')

    logger.info('Checking for null columns in tables...')
    if len(valid_table_cols) > 0:
        for table, col_list in table_col_dict.items():
            try:
                df = spark.read.parquet(os.path.join(output_dir, table), header=True)
                logger.info(f'Table {table} being checked is a parquet table')
            except:
                df = spark.read.csv(os.path.join(output_dir, table), header=True)
                logger.info(f'Table {table} being checked is a csv table...')
            if len(check_null_columns(spark, df, col_list)) != 0 and check_null_columns(spark, df, col_list)[0] == 'failed':
                logger.error('The null column check failed possibly due to invalid column selection...')
            elif len(check_null_columns(spark, df, col_list)) > 0: 
                logger.info(f'Columns with nulls {col_list}')
                logger.error(f'Table {table} has columns with null values...')
            else:
                logger.info(f'Table {table} has no null values in the primary key(s)...')
    else:
        logger.info('No table columns to check...')
    
    logger.info('Data quality check has completed...')
    logger.info('Time taken to complete job {} minutes'.format((time.time() - t0) / 60))

if  __name__ == '__main__':
    main()

