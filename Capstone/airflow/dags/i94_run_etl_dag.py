from datetime import timedelta
import datetime
import os
import boto3
from botocore.exceptions import ClientError
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.hooks.base_hook import BaseHook
from airflow.operators import (CreateS3BucketOperator, CopyFilesToS3Operator)
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator 
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor 
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator
import configparser
import json

parser = configparser.ConfigParser()
try:
    # parser.read('config/etl_config.cfg')
    parser.read(os.path.join('/Users/home/Documents/dend/Data-Engineering-ND/Capstone/config', 'etl_config.cfg'))
except Exception as e:
    print('Cannot find etl_config.cfg file...')
    self.log.error('Cannot find etl_config.cfg file...')
    raise

aws_region = parser['AWS']['aws_region']

base_dir = parser['LOCAL']['base_dir']
log_dir = parser['LOCAL']['log_dir']
output_dir = parser['LOCAL']['output_dir']
scripts_dir = parser['LOCAL']['scripts_dir']

log_dir = os.path.join(base_dir, log_dir)
output_dir = os.path.join(base_dir, output_dir)
scripts_dir = os.path.join(base_dir, scripts_dir)

# base_dir = parser['DOCKER']['base_dir']
# data_dir = parser['DOCKER']['data_dir']
# dict_dir = parser['DOCKER']['dict_dir']

# config_dir = parser['DOCKER']['config_dir']
# scripts_dir = parser['DOCKER']['scripts_dir']
sas_jar_ver = parser['APP']['sas_jar_ver']
sas_jar_key = parser['APP']['jar_dir']

config_dir = parser['LOCAL']['config_dir']
# scripts_dir = parser['DOCKER']['scripts_dir']

config_dir = os.path.join(base_dir, config_dir)
# scripts_dir = os.path.join(base_dir, scripts_dir)

s3_bucket = parser['S3']['s3_bucket']
s3_sas_key = parser['S3']['s3_sas_key']
s3_csv_key = parser['S3']['s3_csv_key']
s3_dict_key = parser['S3']['s3_dict_key']
s3_scripts_key = parser['S3']['s3_scripts_key']
s3_config_key = parser['S3']['s3_config_key']
s3_output_key = parser['S3']['s3_output_key']
s3_log_key = parser['S3']['s3_log_key']

tables = parser['DQ']['tables']
table_col = parser['DQ']['table_col']

default_args = {
    'owner': 'god',
    'start_date': datetime.datetime.combine(datetime.datetime.today(), datetime.time(0, 0)) - timedelta(days=1), #datetime(2021, 4, 29),  
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'catchup': False,
    'max_active_runs': 1
}

connection = BaseHook.get_connection("aws_credentials")
AWS_ACCESS_KEY_ID = connection.login
AWS_SECRET_ACCESS_KEY = connection.password

JOB_FLOW_OVERRIDES = {
    "Name": "immigrations-emr-cluster",
    "ReleaseLabel": "emr-5.28.0",
    "LogUri": os.path.join("s3://", s3_bucket, "cluster.log"),
    "Applications": [
                        {"Name": "Hadoop"},
                        {"Name": "Spark"}
                    ],
    "Configurations": [
        {
                "Classification": "spark-env",
                "Configurations": [
                    {
                        "Classification": "export",
                        "Properties": {
                                        "PYSPARK_PYTHON": "/usr/bin/python3", "JAVA_HOME": "/usr/lib/jvm/java-1.8.0"
                                    },
                    }
                ],
        }
    ],
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
    # {
    #     "Name": "Move ETL scripts from S3 to HDFS",
    #     "ActionOnFailure": "TERMINATE_CLUSTER",
    #     "HadoopJarStep": {
    #         "Jar": "command-runner.jar",
    #         "Args": [
    #             # "s3-dist-cp",
    #             # "--src=" + os.path.join("s3://", s3_bucket, s3_scripts_key, 'etl.py'),
    #             "aws",
    #             "s3",
    #             "cp",
    #             os.path.join("s3://", s3_bucket, s3_scripts_key, 'etl.py'),
    #             "/home/hadoop/scripts/",
    #             #"--dest=/home/hadoop/scripts/",
    #         ]
    #     }
    # }
    # ,
    # {
    #     "Name": "Copy Jar files from S3 to HDFS",
    #     "ActionOnFailure": "TERMINATE_CLUSTER",
    #     "HadoopJarStep": {
    #         "Jar": "command-runner.jar",
    #         "Args": [
    #             "s3-dist-cp",
    #             "--src=" + os.path.join("s3://", s3_bucket, sas_jar_key),
    #             "--dest=/home/hadoop/.ivy2/jars/",
    #         ]
    #     }
    # }
    # ,
    # {
    #     "Name": "Execute the ETL pipeline job",
    #     "ActionOnFailure": "TERMINATE_CLUSTER",
    #     "HadoopJarStep": {
    #         "Jar": "command-runner.jar",
    #         "Args": [
    #             "spark-submit",
    #             "--packages",
    #             ".".join(sas_jar_ver.split('.')[:-1]),
    #             "--deploy-mode",
    #             "client",  
    #             os.path.join("s3://", s3_bucket, s3_scripts_key, "etl.py"),
    #             "--env=S3",
    #             "--aws-access-key-id=" + AWS_ACCESS_KEY_ID,
    #             "--aws-secret-access-key=" + AWS_SECRET_ACCESS_KEY,
    #         ]
    #     }
    # }
    # ,
    {
        "Name": "Copy log to S3",
        "ActionOnFailure": "TERMINATE_CLUSTER",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "s3-dist-cp",
    #            "--src=/home/hadoop/log/",
                "--src=" + log_dir,
                "--dest=s3://" + s3_bucket + "/" + s3_log_key
            ]
        }
    }
    ,
    {
        "Name": "Copy transformed files to S3",
        "ActionOnFailure": "TERMINATE_CLUSTER",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "s3-dist-cp",
                "--src=" + output_dir,
                "--dest=s3://" + s3_bucket + "/" + s3_output_key
            ]
        }
    }
]

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

def create_s3_resource(service, region, access_key_id, secret_access_key):
    """
    Create S3 resource
    :params service - Any AWS service
    :params region - AWS specific region
    :params access_key_id - AWS credential
    :params secret_access_key - AWS credential
    Returns - A boto3 resource
    """
    s3 = boto3.resource(service, aws_access_key_id=access_key_id,
                    aws_secret_access_key=secret_access_key,
                   region_name=region
    )
    return s3

def check_bucket_exists(s3_resource, bucket_name, **kwargs):
    """
    Check whether bucket exists
    :params s3_resource- An S3 resource object
    :params bucket_name - S3 bucket
    Returns - A boolean
    """
    # return ['Copy_config_files_to_s3'] if s3_resource.Bucket(s3_bucket).creation_date is not None else ['Create_S3_bucket', 'Copy_config_files_to_s3']
    return 'Dummy_task' if s3_resource.Bucket(s3_bucket).creation_date is not None else 'Create_S3_bucket'

def create_s3_bucket(s3_client, bucket_name, region='us-west-2', **kwargs):
    """
    Create an s3 bucket
    :params s3_client - An S3 client object
    :params bucket_name - A unique bucket
    :params region - A valid AWS region
    Returns - None
    """
    try:
        s3_client.create_bucket(Bucket=bucket_name,
                                CreateBucketConfiguration={'LocationConstraint': region})
    except ClientError:
        print('S3 client creation failed...')
        raise
    except Exception as e:
        print('Bucket creation has failed. This is most likely because it already exists...')
        print(e)

s3_client = create_client(
                        "s3",
                        region=aws_region,
                        access_key_id=AWS_ACCESS_KEY_ID,
                        secret_access_key=AWS_SECRET_ACCESS_KEY,
                    )
s3_resource = create_s3_resource(
                        "s3",
                        region=aws_region,
                        access_key_id=AWS_ACCESS_KEY_ID,
                        secret_access_key=AWS_SECRET_ACCESS_KEY,
                    )

with DAG(
        'i94_run_etl',
        default_args=default_args,
        description='Run ETL pipeline with Airflow',
        schedule_interval='@daily'
        ) as dag:
        start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

        # copy_scripts = CopyFilesToS3Operator(
        #     task_id='Copy_scripts_to_s3',
        #     dag=dag,
        #     aws_credentials='aws_credentials',
        #     source_path=scripts_dir,
        #     file_ext='py',
        #     s3_bucket=s3_bucket,
        #     s3_key=s3_scripts_key,
        # )

        check_s3_bucket = BranchPythonOperator(
            task_id='Check_S3_bucket',
            dag=dag,
            python_callable=check_bucket_exists,
            op_kwargs={'s3_resource': s3_resource,
                    'bucket_name': s3_bucket,
                    },
            provide_context=True,
        )

        create_s3_bucket = PythonOperator(
            task_id='Create_S3_bucket',
            dag=dag,
            python_callable=create_s3_bucket,
            op_kwargs={'s3_client': s3_client,
                        'bucket_name': s3_bucket,
                        'region': aws_region,
                        },
            # job_id="{{ task_instance.xcom_pull(check_s3_bucket, key='return_value') }}",
            provide_context=True,
        )

        dummy_task = DummyOperator(
            task_id='Dummy_task',
            dag=dag
        )

        one_success = DummyOperator(
            task_id='one_task_success',
            dag=dag,
            trigger_rule=TriggerRule.ONE_SUCCESS,
        )

        copy_config = CopyFilesToS3Operator(
            task_id='Copy_config_files_to_s3',
            dag=dag,
            aws_credentials='aws_credentials',
            source_path=config_dir,
            file_ext='cfg',
            s3_bucket=s3_bucket,
            s3_key=s3_config_key,
            # trigger_rule='one_success',
            # job_id="{{ task_instance.xcom_pull(check_s3_bucket, key='return_value') }}",
            provide_context=True,
        )

        run_etl_script = BashOperator(
            task_id='Execute_ETL_script',
            dag=dag,
            bash_command='python {{ params.scripts_dir }}/etl.py',
            params = {'scripts_dir': scripts_dir},
            trigger_rule='all_success',
        )

        copy_output = CopyFilesToS3Operator(
            task_id='Copy_output_to_s3',
            dag=dag,
            aws_credentials='aws_credentials',
            source_path=output_dir,
            s3_bucket=s3_bucket,
            s3_key=s3_output_key,
            file_ext='ignore',
        )

        # copy_etl_log = CopyFilesToS3Operator(
        #     task_id='Copy_etl_log_to_s3',
        #     dag=dag,
        #     aws_credentials='aws_credentials',
        #     source_path=log_dir,
        #     s3_bucket=s3_bucket,
        #     s3_key=s3_log_key,
        #     file_ext='log',
        # )

        run_dq_script = BashOperator(
            task_id='Execute_data_quality_script',
            dag=dag,
            bash_command='python {{ params.scripts_dir }}/i94_data_quality_check.py --env=s3 \
                                                --bucket-name={{ params.bucket_name }} \
                                                --aws-access-key-id={{ params.access_key_id }} \
                                                --aws-secret-access-key={{ params.secret_access_key }} \
                                                --tables={{ params.check_tables }} \
                                                --table-col={{ params.table_col }}',
            params = {
                        'scripts_dir': scripts_dir,
                        'bucket_name': s3_bucket,
                        'access_key_id': AWS_ACCESS_KEY_ID,
                        'secret_access_key': AWS_SECRET_ACCESS_KEY,
                        'check_tables': tables,
                        'table_col': table_col,
                    },
        )

        copy_log = CopyFilesToS3Operator(
            task_id='Copy_logs_to_s3',
            dag=dag,
            aws_credentials='aws_credentials',
            source_path=log_dir,
            s3_bucket=s3_bucket,
            s3_key=s3_log_key,
            file_ext='log',
        )

        # start_cluster = EmrCreateJobFlowOperator(
        #     task_id='Start_EMR_cluster',
        #     dag=dag,
        #     job_flow_overrides=JOB_FLOW_OVERRIDES,
        #     aws_conn_id='aws_credentials',
        #     emr_conn_id='emr_default',
        #     region_name=aws_region,
        # )

        # add_steps = EmrAddStepsOperator(
        #     task_id='Add_EMR_steps',
        #     dag=dag,
        #     job_flow_id="{{ task_instance.xcom_pull('Start_EMR_cluster', key='return_value') }}",
        #     aws_conn_id='aws_credentials',
        #     steps=SPARK_STEPS,
        # )

        # copy_etl = EmrStepSensor(
        #     task_id='Copy_ETL_scripts_to_Hadoop',
        #     dag=dag,
        #     job_flow_id="{{ task_instance.xcom_pull(task_ids='Start_EMR_cluster', key='return_value') }}",
        #     step_id="{{ task_instance.xcom_pull(task_ids='Add_EMR_steps', key='return_value')[0] }}",
        #     aws_conn_id='aws_credentials',
        # )

        # copy_jar = EmrStepSensor(
        #     task_id='Copy_Jar_files_to_Hadoop',
        #     dag=dag,
        #     job_flow_id="{{ task_instance.xcom_pull(task_ids='Start_EMR_cluster', key='return_value') }}",
        #     step_id="{{ task_instance.xcom_pull(task_ids='Add_EMR_steps', key='return_value')[1] }}",
        #     aws_conn_id='aws_credentials',
        # )

        # run_etl = EmrStepSensor(
        #     task_id='Execute_ETL_pipeline',
        #     dag=dag,
        #     job_flow_id="{{ task_instance.xcom_pull(task_ids='Start_EMR_cluster', key='return_value') }}",
        #     step_id="{{ task_instance.xcom_pull(task_ids='Add_EMR_steps', key='return_value')[0] }}",
        #     aws_conn_id='aws_credentials',
        # )

        # copy_output = EmrStepSensor(
        #     task_id='Copy_output_to_S3',
        #     dag=dag,
        #     job_flow_id="{{ task_instance.xcom_pull(task_ids='Start_EMR_cluster', key='return_value') }}",
        #     step_id="{{ task_instance.xcom_pull(task_ids='Add_EMR_steps', key='return_value')[0] }}",
        #     aws_conn_id='aws_credentials',
        # )

        # copy_log = EmrStepSensor(
        #     task_id='Copy_log_to_S3',
        #     dag=dag,
        #     job_flow_id="{{ task_instance.xcom_pull(task_ids='Start_EMR_cluster', key='return_value') }}",
        #     step_id="{{ task_instance.xcom_pull(task_ids='Add_EMR_steps', key='return_value')[1] }}",
        #     aws_conn_id='aws_credentials',
        # )

        # stop_cluster = EmrTerminateJobFlowOperator(
        #     task_id='Stop_EMR_clsuter',
        #     dag=dag,
        #     job_flow_id="{{ task_instance.xcom_pull(task_ids='Start_EMR_cluster', key='return_value') }}",
        #     aws_conn_id='aws_credentials',
        # )

        end_operator = DummyOperator(task_id='End_execution', dag=dag)

        start_operator >> check_s3_bucket >> [create_s3_bucket, dummy_task]
        [dummy_task, create_s3_bucket] >> one_success
        one_success >> copy_config >> run_etl_script >> copy_output  >> run_dq_script >> copy_log >> end_operator

        # create_s3_bucket >> copy_config
        # copy_config >> copy_output  >> run_dq_script >> copy_log >> end_operator
        # create_s3_bucket >> copy_config

        # start_operator >> [copy_scripts, copy_config]

        # [copy_scripts, copy_config] >> start_cluster

        # start_cluster >> add_steps >> run_etl >> copy_log >> stop_cluster >> end_operator

        # start_cluster >> add_steps >> copy_jar >> copy_etl >> run_etl >> copy_log >> stop_cluster >> end_operator
