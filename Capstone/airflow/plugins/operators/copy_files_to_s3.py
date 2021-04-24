from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import os

class CopyFilesToS3Operator(BaseOperator):
    """
    Custom operator to copy files to S3 buckets
    :aws_credentials_id - AWS Credentials
    :source_path - Source file path on local system
    :s3_bucket - S3 bucket name
    :s3_key - Folder name under the bucket
    """
    ui_color = '#7591f0'
    
    @apply_defaults
    def __init__(self, aws_credentials_id, 
                 source_path,
                 file_ext, 
                 s3_bucket, 
                 s3_key,
                 *args,
                 **kwargs):
        super(CopyFilesToS3Operator, self).__init__(*args, **kwargs)
        self.aws_creds = aws_credentials
        self.source_path = source_path
        self.file_ext = file_ext
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
    
    def execute(self, context):
        try:
            s3_hook = S3Hook(self.aws_creds)
        except Exception as e:
            self.log.error('Invalid AWS credentials...')
            raise
        for file in os.listdir(self.source_path):
            try:
                if file.endswith(self.file_ext):
                    # file = file.split('/')[-1]
                    self.log.info(f'Copied file {file} into S3 bucket...')
                    s3_hook.load_file(filename=file, bucket_name=s3.bucket,
                             key=self.s3_key, replace=True)
                else:
                    self.log.info(f'File {file} doesnt end with extension {self.file_ext}')
            except Exception as e:
                self.log.warn(f'Failed to copy {file} into S3 bucket...')
            