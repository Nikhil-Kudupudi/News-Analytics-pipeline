import logging
import boto3
from botocore.exceptions import ClientError

from utils.config_loader import get_config



class AWSUtils:
    def __init__(self):
        self.bucket_name=get_config("aws","bucket_name")
        self.region=get_config("aws","region")
        self.s3_client = boto3.client('s3')

    def create_bucket(self,bucket_name, region='us-east-1'):
        """Create an S3 bucket in a specified region

        If a region is not specified, the bucket is created in the S3 default
        region (us-east-1).

        :param bucket_name: Bucket to create
        :param region: String region to create bucket in, e.g., 'us-west-2'
        :return: True if bucket created, else False
        """

        # Create bucket
        try:
            if region is None:
                
                buckets=self.s3_client.list_buckets()
                # for bucket in buckets['Buckets']:
                    
                s3_client.create_bucket(Bucket=bucket_name)
            else:
                s3_client = boto3.client('s3', region_name=region)
                location = {'LocationConstraint': region}
                s3_client.create_bucket(Bucket=bucket_name,
                                        CreateBucketConfiguration=location)
        except ClientError as e:
            logging.error(e)
            return False
        return True
    def create_folder(self,foldername):
        try:
            if not foldername.endswith("/"):
                foldername+="/"
            self.s3_client.put_object(Bucket=self.bucket_name,Key="files/"+foldername)
        except ClientError as e:
            raise Exception(e)


if __name__=='__main__':
    aws=AWSUtils()
    aws.create_folder("geteverytest")