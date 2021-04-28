from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (CreateS3BucketOperator, CopyFilesToS3Operator)
from airflow.operators.postgres_operator import PostgresOperator
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator 
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor 
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator
import configparser
import json

parser = configparser.ConfigParser()
try:
    parser.read('config/etl_config.cfg')
    # parser.read(os.path.join('/Users/home/Documents/dend/Data-Engineering-ND/Capstone/config', 'etl_config.cfg'))
except Exception as e:
    print('Cannot find etl_config.cfg file...')
    self.log.error('Cannot find etl_config.cfg file...')
    raise

aws_region = parser['AWS']['aws_region']

base_dir = parser['DOCKER']['base_dir']
data_dir = parser['DOCKER']['data_dir']
dict_dir = parser['DOCKER']['dict_dir']
airflow_dir = parser['DOCKER']['airflow_dir']
config_dir = parser['DOCKER']['config_dir']
scripts_dir = parser['DOCKER']['scripts_dir']

config_dir = os.path.join(airflow_dir, config_dir)
scripts_dir = os.path.join(airflow_dir, scripts_dir)

s3_bucket = parser['S3']['s3_bucket']
s3_sas_key = parser['S3']['s3_sas_key']
s3_csv_key = parser['S3']['s3_csv_key']
s3_dict_key = parser['S3']['s3_dict_key']
s3_scripts_key = parser['S3']['s3_scripts_key']
s3_config_key = parser['S3']['s3_config_key']
s3_log_key = parser['S3']['s3_log_key']

default_args = {
    'owner': 'god',
    'start_date': datetime(2021, 4, 27),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'catchup': False,
    'max_active_runs': 1
}

JOB_FLOW_OVERRIDES = {
    "Name": "immigrations-emr-cluster",
    "ReleaseLabel": "emr-5.29.0",
    "LogUri": os.path.join("s3://", s3_bucket, "cluster.log"),
    # "Application": [
    #                     {"Name": "Hadoop"},
    #                     {"Name": "Spark"}
    #                 ],
    # "Configuration": [
    #     {
    #             "Classification": "spark-env",
    #             "Configuration": [
    #                 {
    #                     "Classification": "export",
    #                     "Properties": {
    #                                     "PYSPARK_PYTHON": "/usr/bin/python3"
    #                                 },
    #                 }
    #             ],
    #     }
    # ],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master node",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m4.xlarge",
                "InstanceCount": 1
            },
            {
                "Name": "Core-2",
                "Market": "ON_DEMAND",
                "InstanceRole": "CORE",
                "InstanceType": "m4.xlarge",
                "InstanceCount": 2
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole"
}

SPARK_STEPS = [
    {
        "Name": "Move ETL scripts from S3 to HDFS",
        "ActionOnFailure": "TERMINATE_CLUSTER",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "aws",
                "s3",
                "cp",
                "s3://" + s3_bucket + "/" + s3_scripts_key,
                "/home/hadoop/"
            ]
        }
    }
    ,
    #     {
    #     'Name': 'Copy config from Docker to HDFS',
    #     'ActionOnFailure': 'CANCEL_AND_WAIT',
    #     'HadoopJarStep': {
    #         'Jar': 'command-runner.jar',
    #         'Args': [
    #             'sudo',
    #             'cp',
    #             '/usr/local/airflow/config/etl_config.cfg',
    #             '/tmp'
    #         ]
    #     }
    # }
    # ,
    # {
    #     'Name': 'Move CSV data from S3 to HDFS',
    #     'ActionOnFailure': 'CANCEL_AND_WAIT',
    #     'HadoopJarStep': {
    #         'Jar': 'command-runner.jar',
    #         'Args': [
    #             's3-dist-cp',
    #             '--src=s3://' + s3_bucket + '/' + s3_csv_key,
    #             '--dest=/home/hadoop/'
    #         ]
    #     }
    # }
    # ,
    # {
    #     'Name': 'Move data dictionary from S3 to HDFS',
    #     'ActionOnFailure': 'CANCEL_AND_WAIT',
    #     'HadoopJarStep': {
    #         'Jar': 'command-runner.jar',
    #         'Args': [
    #             's3-dist-cp',
    #             '--src=s3://' + s3_bucket + '/' + s3_dict_key,
    #             '--dest=/home/hadoop/'
    #         ]
    #     }
    # }
    # ,
    # {
    #     'Name': 'Move config file from S3 to HDFS',
    #     'ActionOnFailure': 'CANCEL_AND_WAIT',
    #     'HadoopJarStep': {
    #         'Jar': 'command-runner.jar',
    #         'Args': [
    #             's3-dist-cp',
    #             '--src=s3://' + s3_bucket + '/' + s3_config_key,
    #             '--dest=/home/hadoop/'
    #         ]
    #     }
    # }
    # ,
    {
        "Name": "Execute the ETL pipeline job",
        "ActionOnFailure": "TERMINATE_CLUSTER",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--packages",
                "saurfang:saurfang:spark-sas7bdat:3.0.0-s_2.12",
                "/home/hadoop/scripts/etl.py",
            ]
        }
    }
    ,
    {
        "Name": "Copy log to S3",
        "ActionOnFailure": "TERMINATE_CLUSTER",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "s3-dist-cp",
                "--src=/home/hadoop/log/",
                "--dest=s3://" + s3_bucket + "/" + s3_log_key
            ]
        }
    }
]

with DAG(
        'i94_run_etl',
        default_args=default_args,
        description='Run ETL pipeline with Airflow',
        schedule_interval='@daily'
        ) as dag:
        start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

        copy_scripts = CopyFilesToS3Operator(
            task_id='Copy_scripts_to_s3',
            dag=dag,
            aws_credentials='aws_credentials',
            source_path=scripts_dir,
            file_ext='py',
            s3_bucket=s3_bucket,
            s3_key=s3_scripts_key,
        )

        copy_config = CopyFilesToS3Operator(
            task_id='Copy_config_files_to_s3',
            dag=dag,
            aws_credentials='aws_credentials',
            source_path=config_dir,
            file_ext='cfg',
            s3_bucket=s3_bucket,
            s3_key=s3_config_key,
        )

        start_cluster = EmrCreateJobFlowOperator(
            task_id='Start_EMR_cluster',
            dag=dag,
            job_flow_overrides=JOB_FLOW_OVERRIDES,
            aws_conn_id='aws_credentials',
            emr_conn_id='emr_default',
            region_name=aws_region,
        )

        add_steps = EmrAddStepsOperator(
            task_id='Add_EMR_steps',
            dag=dag,
            job_flow_id="{{ task_instance.xcom_pull('Start_EMR_cluster', key='return_value') }}",
            aws_conn_id='aws_credentials',
            steps=SPARK_STEPS,
        )

        copy_etl = EmrStepSensor(
            task_id='Copy_ETL_scripts_to_Hadoop',
            dag=dag,
            job_flow_id="{{ task_instance.xcom_pull(task_ids='Start_EMR_cluster', key='return_value') }}",
            step_id="{{ task_instance.xcom_pull(task_ids='Add_EMR_steps', key='return_value')[0] }}",
            aws_conn_id='aws_credentials',
        )

        run_etl = EmrStepSensor(
            task_id='Execute_ETL_pipeline',
            dag=dag,
            job_flow_id="{{ task_instance.xcom_pull(task_ids='Start_EMR_cluster', key='return_value') }}",
            step_id="{{ task_instance.xcom_pull(task_ids='Add_EMR_steps', key='return_value')[1] }}",
            aws_conn_id='aws_credentials',
        )

        copy_log = EmrStepSensor(
            task_id='Copy_log_to_S3',
            dag=dag,
            job_flow_id="{{ task_instance.xcom_pull(task_ids='Start_EMR_cluster', key='return_value') }}",
            step_id="{{ task_instance.xcom_pull(task_ids='Add_EMR_steps', key='return_value')[2] }}",
            aws_conn_id='aws_credentials',
        )

        stop_cluster = EmrTerminateJobFlowOperator(
            task_id='Stop_EMR_clsuter',
            dag=dag,
            job_flow_id="{{ task_instance.xcom_pull(task_ids='Start_EMR_cluster', key='return_value') }}",
            aws_conn_id='aws_credentials',
        )

        end_operator = DummyOperator(task_id='End_execution', dag=dag)

        start_operator >> [copy_scripts, copy_config]

        [copy_scripts, copy_config] >> start_cluster

        start_cluster >> add_steps >> copy_etl >> run_etl >> copy_log >> stop_cluster >> end_operator
