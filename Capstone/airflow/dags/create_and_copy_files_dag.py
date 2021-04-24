from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from operators import (CreateS3BucketOperator, CopyFilesToS3Operator)
from airflow.providers.postgres.operators.postgres import PostgresOperator
import configparser

parser = configparser.ConfigParser()
parser.read('../../config/etl_config.cfg')

aws_region = parser['AWS']['aws_region']
base_dir = parser['LOCAL']['base_dir']
s3_bucket = parser['S3']['s3_bucket']
s3_raw_key = parser['S3']['s3_raw_key']
s3_csv_key = parser['S3']['s3_csv_key']
s3_dict_key = parser['S3']['s3_dict_key']

default_args = {
    'owner': 'god',
    'start_date': datetime(2021, 4, 24),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'catchup': False,
    'max_active_runs': 1
}

with DAG(
        'Immigrations',
        default_args=default_args,
        description='Create and copy files to S3 bucket with Airflow',
        schedule_interval='0 * * * *'
        ) as dag:
        start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)
        
        create_s3_bucket = CreateS3BucketOperator(
            task_id='Create_s3_bucket',
            dag=dag,
            aws_credentials='aws_credentials',
            s3_bucket=s3_bucket,
            region=aws_region,
        )
        
        copy_sas_files = CopyFilesToS3Operator(
            task_id='Copy_raw_sas_files',
            dag=dag,
            aws_credentials='aws_credentials',
            source_path=base_dir,
            file_ext='sas7bdat',
            s3_bucket=s3_bucket,
            s3_key=s3_raw_key
        )
        
        copy_csv_files = CopyFilesToS3Operator(
            task_id='Copy_csv_files',
            dag=dag,
            aws_credentials='aws_credentials',
            source_path=base_dir,
            file_ext='csv',
            s3_bucket=s3_bucket,
            s3_key=s3_csv_key
        )
        
        copy_data_dictionary = CopyFilesToS3Operator(
            task_id='Copy_data_dictionary',
            dag=dag,
            aws_credentials='aws_credentials',
            source_path=base_dir,
            file_ext='SAS',
            s3_bucket=s3_bucket,
            s3_key=s3_dict_key
        )
        
        end_operator = DummyOperator(task_id='End_execution', dag=dag)
        
        start_operator >> create_s3_bucket
        
        create_s3_bucket >> [copy_sas_files, copy_csv_files, copy_data_dictionary]
        
        [copy_sas_files, copy_csv_files, copy_data_dictionary] << end_operator