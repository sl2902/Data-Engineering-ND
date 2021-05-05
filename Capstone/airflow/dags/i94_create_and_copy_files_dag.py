from datetime import timedelta
import datetime
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (CreateS3BucketOperator, CopyFilesToS3Operator)
# from operators.create_s3_bucket import CreateS3BucketOperator
# from operators.copy_files_to_s3 import CopyFilesToS3Operator
from airflow.operators.postgres_operator import PostgresOperator
import configparser
import json

parser = configparser.ConfigParser()
# cfg_rel_path = os.path.dirname(os.path.realpath(__file__))
# print(cfg_rel_path)
# f = os.path.relpath('/Users/home/Documents/dend/Data-Engineering-ND/Capstone/config/etl_config.cfg', cfg_rel_path)
# print(f)
try:
    # parser.read('config/etl_config.cfg')
    parser.read('/Users/home/Documents/dend/Data-Engineering-ND/Capstone/config/etl_config.cfg')
except Exception as e:
    print('Cannot find etl_config.cfg file...')
    self.log.error('Cannot find etl_config.cfg file...')
    raise

aws_region = parser['AWS']['aws_region']
# base_dir = parser['LOCAL']['base_dir']
# data_dir = parser['LOCAL']['data_dir']
# dict_dir = parser['LOCAL']['dict_dir']

base_dir = parser['DOCKER']['base_dir']
data_dir = parser['DOCKER']['data_dir']
dict_dir = parser['DOCKER']['dict_dir']

data_dir = os.path.join(base_dir, data_dir)
dict_dir = os.path.join(base_dir, dict_dir)
# files = json.loads(parser['LOCAL']['input_files'])
files = json.loads(parser['DOCKER']['input_files'])

s3_bucket = parser['S3']['s3_bucket']
s3_sas_key = parser['S3']['s3_sas_key']
s3_csv_key = parser['S3']['s3_csv_key']
s3_dict_key = parser['S3']['s3_dict_key']

default_args = {
    'owner': 'god',
    'start_date': datetime.datetime.combine(datetime.datetime.today(), datetime.time(0, 0)) - timedelta(days=1), #datetime.datetime(2021, 4, 30),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'catchup': False,
    'max_active_runs': 1
}

with DAG(
        'i94_copy_files_to_s3',
        default_args=default_args,
        description='Create and copy files to S3 bucket with Airflow',
        schedule_interval='@daily'
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
            source_path=data_dir,
            file_ext='sas7bdat',
            s3_bucket=s3_bucket,
            s3_key=s3_sas_key,
            src_files = files
        )
        
        copy_csv_files = CopyFilesToS3Operator(
            task_id='Copy_csv_files',
            dag=dag,
            aws_credentials='aws_credentials',
            source_path=data_dir,
            file_ext='csv',
            s3_bucket=s3_bucket,
            s3_key=s3_csv_key
        )
        
        copy_data_dictionary = CopyFilesToS3Operator(
            task_id='Copy_data_dictionary',
            dag=dag,
            aws_credentials='aws_credentials',
            source_path=dict_dir,
            file_ext='SAS',
            s3_bucket=s3_bucket,
            s3_key=s3_dict_key
        )
        
        end_operator = DummyOperator(task_id='End_execution', dag=dag)
        
        start_operator >> create_s3_bucket
        
        create_s3_bucket >> [copy_sas_files, copy_csv_files, copy_data_dictionary]
        
        [copy_sas_files, copy_csv_files, copy_data_dictionary] >> end_operator