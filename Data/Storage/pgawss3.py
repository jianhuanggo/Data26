import os
from types import SimpleNamespace
import boto3
from botocore.exceptions import ClientError
from Data.Utils import pgyaml
from Data.Utils import pgsecret
from Data.Storage import pgstoragebase


CONFIG_FILE_PATH = '/Users/jianhuang/test100/New/Config_yaml/'


class PGAWSS3(pgstoragebase.PGStorageBase):
    def __init__(self, logger=None, region_name=None):
        """
        if region is not specified, it will default to region (us-east-1).
        """
        try:
            self._logger = logger
            if region_name is None:
                region_name = "us-east-1"
            self._region_name = region_name
            config_yaml = SimpleNamespace(**pgyaml.yaml_load(
                yaml_filename=f"{CONFIG_FILE_PATH}/{__class__.__name__}.yaml"))
            self._s3_client = boto3.Session(aws_access_key_id=config_yaml.aws_access_key_id,
                                            aws_secret_access_key=config_yaml.aws_secret_access_key,
                                            region_name=self._region_name).client('s3')

            #sels3_resource = session.resource('s3')
        except Exception as err:
            raise (f"Something wrong while uploading file to S3: {err}")

    def load_config(self):
        pass

    def create_bucket(self, bucket_name: str) -> bool:
        """
        Create an S3 bucket in a specified region which is specified when this object is created.
        The bucket encryption defaults to AWS S3 default encryption

        :param bucket_name: Bucket to create
        :return: True if bucket created, else False
        """

        try:
            if self._region_name == "us-east-1":
                self._s3_client.create_bucket(Bucket=bucket_name)
            else:
                self._s3_client.create_bucket(Bucket=bucket_name,
                                              CreateBucketConfiguration={'LocationConstraint': self._region_name})
            self._s3_client.put_bucket_encryption(Bucket=bucket_name,
                                                  ServerSideEncryptionConfiguration={'Rules': [{'ApplyServerSideEncryptionByDefault': {'SSEAlgorithm': 'AES256'}}]})

        except ClientError as err:
            if not self._logger:
                print(err)
            else:
                self._logger(err)
            return False
        return True

    def load(self):
        pass

    def save(self, source_filename: str, s3_bucket_location: str, object_key: str):
        if s3_bucket_location[-1] == '/':
            s3_bucket_location = s3_bucket_location[:-1]
        #return self._s3_client.meta.client.upload_file(source_filename, s3_bucket_location, object_key)
        return self._s3_client.upload_file(source_filename, s3_bucket_location, object_key)


if __name__ == '__main__':
    random_string = pgsecret.generate_secret(16)
    random_file = f"/Users/jianhuang/test100/New/temp/file_{random_string}"
    with open(random_file, 'wb') as fout:
        fout.write(os.urandom(1024))

    mys3 = PGAWSS3()
    mys3.create_bucket(f"test{random_string}")
    mys3.save(f"/Users/jianhuang/test100/New/temp/file_{random_string}",
                     f"test{random_string}",
                     f"file_{random_string}")
    #AWSs3().create_bucket("test", region='us-east-2')
