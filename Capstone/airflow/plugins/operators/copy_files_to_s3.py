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
    :src_files - Optional list of files
    """
    ui_color = '#7591f0'
    
    @apply_defaults
    def __init__(self, aws_credentials, 
                 source_path, 
                 s3_bucket, 
                 s3_key,
                 file_ext=None,
                 src_files=None,
                 *args,
                 **kwargs):
        super(CopyFilesToS3Operator, self).__init__(*args, **kwargs)
        self.aws_creds = aws_credentials
        self.source_path = source_path
        self.file_ext = file_ext
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.src_files = src_files
    
    def execute(self, context):
        try:
            s3_hook = S3Hook(self.aws_creds)
        except Exception as e:
            self.log.error('Invalid AWS credentials...')
            raise
        for path, _, files in os.walk(self.source_path):
            try:
                for _file in files:
                    if self.src_files is not None:
                        if _file in self.src_files:
                            s3_hook.load_file(os.path.join(path, _file), os.path.join(self.s3_key, _file), 
                                        bucket_name=self.s3_bucket, replace=True)
                            self.log.info(f'Copied file {_file} into S3 bucket...')
                        # else:
                        #     self.log.info(f'File {_file} is not in the list specified...')
                    else:
                        if _file.endswith(self.file_ext):
                            s3_hook.load_file(os.path.join(path, _file), os.path.join(self.s3_key, _file), 
                                        bucket_name=self.s3_bucket, replace=True)
                            self.log.info(f'Copied file {_file} into S3 bucket...')
                        else:
                            # default option caters to files with any extension
                            # useful when the transformed output may have different formats
                            if _file == '.DS_Store': continue
                            s3_hook.load_file(os.path.join(path, _file), os.path.join(self.s3_key, path.split('/')[-1], _file), 
                                        bucket_name=self.s3_bucket, replace=True)
                            self.log.info(f'Copied file {_file} into S3 bucket...')
                        # else:
                        #     self.log.info(f"File {_file} doesn't end with extension {self.file_ext}")
            except Exception as e:
                self.log.error(f'Failed to copy {_file} into S3 bucket...')
            