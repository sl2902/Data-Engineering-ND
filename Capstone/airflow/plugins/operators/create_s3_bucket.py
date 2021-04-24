from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import os

class CreateS3BucketOperator(BaseOperator):
    """
    Custom operator to create S3 buckets
    :aws_credentials- AWS Credentials
    :s3_bucket - S3 bucket name
    :s3_key - Folder name under the bucket
    :region - AWS region where bucket is located
    """
    ui_color = '#358140'
    
    @apply_defaults
    def __init__(self, aws_credentials, s3_bucket, region, *args, **kwargs):
        super(CreateS3BucketOperator, self).__init__(*args, **kwargs)
        self.aws_creds = aws_credentials
        self.s3_bucket = s3_bucket
        self.region = region
    
    def execute(self, context):
        path = self.s3_bucket
        try:
            s3_hook = S3Hook(self.aws_creds)
        except Exception as e:
            self.log.error('Invalid AWS credentials...')
            raise
        if s3.hook.check_for_bucket(bucket_name=path):
            self.log.info(f'{path} already exists...')
        else:
            try:
                s3_hook.create_bucket(bucket_name=path, region=self.region)
                self.log.info(f'{path} has been created...')
            except Exception as e:
                self.log.error('Bucket creation failed...')
                raise
    
