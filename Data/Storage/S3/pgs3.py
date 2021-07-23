import io
import os
import inspect
from typing import Callable, Union, Any, TypeVar, Tuple, Iterable, Generator
import pandas as pd
from pprint import pprint
import shutil
from types import SimpleNamespace
import boto3
import requests
import pandas as pd
from botocore.exceptions import ClientError
from Data.Storage import pgstoragebase
from Data.StorageFormat import pgstorageformat
from Meta import pggenericfunc
from Data.Utils import pgdirectory
from Data.Utils import pgfile
from Data.Utils import pgsecret
from Data.Utils import pgoperation
from Data.Storage import pgstoragecommon
from Meta.pggenericfunc import check_args


__version__ = "1.7"


class PGS3(pgstoragebase.PGStorageBase, pgstoragecommon.PGStorageCommon):
    def __init__(self, project_name: str = "s3", logging_enable: str = False):

        super(PGS3, self).__init__(project_name=project_name,
                                   object_short_name="PG_S3",
                                   config_file_pathname=__file__.split('.')[0] + ".ini",
                                   logging_enable=logging_enable,
                                   config_file_type="ini")

        ### Common Variables
        self._storage_type = "s3"
        """
        mode: 1) file
              2) direct
        Example of storage_paramter:
        { 
          's3_bucket_location': " ", 
          'object_key': " ",
          'mode': file,
          'directory': " ",
          'filename': " "
        }
        """


        ### Specific Variables
        self._region_name = "us-east-1"
        self._storage_prefix = "s3://"
        self._s3_client = None
        self._save_size_map = {"direct_default": self.save_direct_default,
                               "direct_large": self.save_streaming_multiload,
                               "file_default": self.save_file_default,
                               "file_large": self.save_streaming_multiload
                               }
        self._load_size_map = {"direct_default": self.load_direct_default,
                               "direct_large": self.load_streaming_multiload,
                               "file_default": self.load_file_default,
                               "file_large": self.load_streaming_multiload
                               }

        self.create_client()
        self.set_bucket()
        self._data_inputs = {}
        self._data = {}

    def inst(self):
        return self

    @property
    def data(self):
        return self._data

    @property
    def storage_type(self):
        return self._storage_type

    @property
    def storage_parameter(self):
        return self._storage_parameter

    @property
    def s3_client(self):
        return self._s3_client

    def get_storage_name_and_path(self, dirpath: str) -> Tuple[Union[str, None], Union[str, None]]:
        try:
            if not dirpath.startswith(self._storage_prefix):
                return None, None
            _path_parse = dirpath.replace(self._storage_prefix, "").strip().split('/')
            return _path_parse[0] if _path_parse and _path_parse[0] != "" else None, None if _path_parse and len(_path_parse) == 1 else '/'.join(_path_parse[1:])

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
        return None, None

    def create_client(self, aws_access_key_id: str = None, aws_secret_access_key: str = None, region_name=None) -> bool:
        _region_name = region_name or self._region_name or "us-east-1"
        _aws_access_key_id = aws_access_key_id or self._config.parameters["config_file"]['default']['aws_access_key_id']
        _aws_secret_access_key = aws_secret_access_key or self._config.parameters["config_file"]['default']['aws_secret_access_key']

        try:
            self._s3_client = boto3.Session(aws_access_key_id=_aws_access_key_id,
                                            aws_secret_access_key=_aws_secret_access_key,
                                            region_name=_region_name).client('s3')
            return True
        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return False

    """
    def parse_location(self, s3_object_key: str) -> dict:
        if not s3_object_key:
            return {}

        _s3_object_key = s3_object_key.replace("s3://", "") if s3_object_key.startswith("s3://") else s3_object_key

        if "/" not in _s3_object_key or _s3_object_key[-1] == "/":
            _response = self._s3_client.list_objects_v2(Bucket=_s3_object_key.replace("/", ""), Prefix="")
            return {"s3_bucket_location": _s3_object_key.replace("/", ""),
                    "object_key": [x["Key"] for x in _response["Contents"] if x]} if _response['ResponseMetadata']['HTTPStatusCode'] == 200 else {}

        else:
            try:
                _dir_parts = _s3_object_key.split('/')
                _bucket_name = _dir_parts[0]
                _object_key = '/'.join(_dir_parts[1:])

                print(_bucket_name)
                print(_object_key)
                print(self._s3_client)

                _response = self._s3_client.head_object(Bucket=_bucket_name,
                                                        Key=_object_key)

                if _response['ResponseMetadata']['HTTPStatusCode'] == 200:
                    return {"s3_bucket_location": _bucket_name, "object_key": _object_key}
                else:
                    return {}
            except ClientError as err:
                pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
                return {}
    """

    def parse_location(self, s3_object_key: str) -> dict:

        if not s3_object_key:
            return {}

        try:
            _s3_object_key = s3_object_key.replace("s3://", "") if s3_object_key.startswith("s3://") else s3_object_key
            _dir_parts = _s3_object_key.split('/')
            _bucket_name = _dir_parts[0]
            _object_key = '/'.join(_dir_parts[1:]) if len(_dir_parts) > 1 else ""

            #print(_bucket_name)
            #print(_object_key)
            #print(self._s3_client)

            _response = self._s3_client.list_objects_v2(Bucket=_bucket_name, Prefix=_object_key)

            return {"s3_bucket_location": _s3_object_key.split('/')[0],
                    "object_key": [x["Key"] for x in _response["Contents"] if x]} if _response['ResponseMetadata'][
                                                                                         'HTTPStatusCode'] == 200 and "Contents" in _response else {}
        except ClientError as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return {}


    def create_bucket(self, s3_bucket_location: str) -> bool:
        """
        Create an S3 bucket in a specified region which is specified when this object is created.
        The bucket encryption defaults to AWS S3 default encryptio
        :param bucket_name: Bucket to create
        :return: True if bucket created, else False
        """

        try:
            if self._region_name == "us-east-1":
                #print(s3_bucket_location)
                self._s3_client.create_bucket(Bucket=s3_bucket_location)
            else:
                self._s3_client.create_bucket(Bucket=s3_bucket_location,
                                              CreateBucketConfiguration={'LocationConstraint': self._region_name})

            self._s3_client.put_bucket_encryption(Bucket=s3_bucket_location,
                                                  ServerSideEncryptionConfiguration={'Rules':
                                                      [{'ApplyServerSideEncryptionByDefault': {'SSEAlgorithm': 'AES256'}}]})
        except ClientError as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return False
        return True

    def set_bucket(self, s3_bucket_location: str = None) -> bool:
        try:
            _s3_bucket_location = s3_bucket_location or\
                                  self._storage_parameter['s3_bucket_location'] if "s3_bucket_location" \
                                  in self._storage_parameter \
                                  else self._config.parameters["config_file"]['default']['s3_bucket_location']
            #print(f"s3 bucket location: {_s3_bucket_location}")
            self.set_param(storage_parameter={**self._storage_parameter,
                                              **{'s3_bucket_location': _s3_bucket_location}})
        except ClientError as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return False
        return True

    def delete_s3_obj(self, bucket_name: str, key: str) -> bool:
        try:
            self._s3_client.delete_object(Bucket=bucket_name, Key=key)
            return True
        except ClientError as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return False

    def save(self, data: object, storage_format: str = None, storage_parameter: dict = None, *args, **kwargs) -> bool:

        _storage_parameter = {**self._storage_parameter, **storage_parameter} if storage_parameter else self._storage_parameter
        pprint(_storage_parameter)

        if not pggenericfunc.pg_errorcheck_exist(f"s3_bucket_location mode", _storage_parameter):
            pggenericfunc.pg_error_logger(self._logger,
                                          inspect.currentframe().f_code.co_name,
                                          "Storage parameter: s3_bucket_location or mode is not set")
            return False


        ### Decide whether data needs to be encoded and whether method
        ### Load up requested storage format if it is available
        if storage_format:
            if storage_format not in self._storage_format_instance:
                with pgstorageformat.set_storage_format(object_type=storage_format, storage_instance={self._name: self}) as session:
                    self._storage_format_instance[storage_format] = session

            try:
                _result = self._storage_format_instance[storage_format].encode(data=data,
                                                                               storage_parameter=_storage_parameter)
                print(_result)
            except Exception as err:
                pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
        elif _storage_parameter['mode'] == "file":
            _storage_parameter['directory'], _storage_parameter['filename'] = pgdirectory.get_dir_filename_from_dirpath(data)
            #print(_storage_parameter['directory'], _storage_parameter['filename'])

            return self._save_size_map[f"{_storage_parameter['mode']}_default"](data, _storage_parameter)
        elif isinstance(data, str):
            #default encoder
            _result = io.BytesIO()
            _result.write(data.encode())
            # Reset read pointer. DOT NOT FORGET THIS, else all uploaded files will be empty!
            _result.seek(0)
            print(_result)
        else:
            pggenericfunc.pg_error_logger(self._logger,
                                          inspect.currentframe().f_code.co_name,
                                          f"{type(data)} is currently not supported")
            return False

        #print(f"s3 storage parameter: {self._storage_parameter}")
        return self._save_size_map[f"{_storage_parameter['mode']}_default"](_result)

    def save_file_default(self, data: object, storage_parameter: dict = None, *args, **kwargs) -> bool:
        _storage_parameter = {**self._storage_parameter, **storage_parameter} if storage_parameter else self._storage_parameter

        if not pggenericfunc.pg_errorcheck_exist(f"s3_bucket_location directory object_key", _storage_parameter):
            pggenericfunc.pg_error_logger(self._logger,
                                          inspect.currentframe().f_code.co_name,
                                          "Storage parameter: s3_bucket_location or directory is not set")
            return False

        try:

            if "filename" not in _storage_parameter:
                _dirpathfilename = pgdirectory.add_splash_2_dir(_storage_parameter['directory'])
                for filename in pgdirectory.files_in_dir(_dirpathfilename):
                    self._s3_client.upload_file(_dirpathfilename + filename,
                                                pgdirectory.remove_end_splash_from_dir(_storage_parameter["s3_bucket_location"]),
                                                filename)
            else:
                _dirpathfilename = pgdirectory.add_splash_2_dir(_storage_parameter['directory']) + _storage_parameter['filename']
                self._s3_client.upload_file(_dirpathfilename,
                                            pgdirectory.remove_end_splash_from_dir(_storage_parameter["s3_bucket_location"]),
                                            _storage_parameter['object_key'])


            """
            _object_key = ("object_key" in self._storage_parameter and self._storage_parameter['object_key']) or \
                          self._storage_parameter['filename'].replace("*", pgfile.get_random_string())
            """
            return True

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger,
                                          inspect.currentframe().f_code.co_name,
                                          err)

            return False

    """
    def save(self, source_filename: str, s3_bucket_location: str, object_key: str):
        if s3_bucket_location[-1] == '/':
            s3_bucket_location = s3_bucket_location[:-1]
        # return self._s3_client.meta.client.upload_file(source_filename, s3_bucket_location, object_key)
        return self._s3_client.upload_file(source_filename, s3_bucket_location, object_key)
    """

    def save_direct_default(self, data: io.BytesIO, *args, **kwargs) -> bool:
        if not pggenericfunc.pg_errorcheck_exist(f"s3_bucket_location object_key", self._storage_parameter):
            pggenericfunc.pg_error_logger(self._logger,
                                          inspect.currentframe().f_code.co_name,
                                          "Storage parameter: s3_bucket_location or object_key is not set")
            return False
        ### Create content in respective S3 bucket from BytesIO data passed in
        try:
            #print(f"in save_direct_default: {data.read().decode()}")
            data.seek(0)
            # Upload the file. "MyDirectory/test.txt" is the name of the object to create
            # boto_test_bucket.upload_fileobj(buf, "MyDirectory/test.txt")
            self._s3_client.upload_fileobj(data,
                                           self._storage_parameter["s3_bucket_location"],
                                           self._storage_parameter["object_key"])
            print(f"successfully uploaded {self._storage_parameter['object_key']} to {self._storage_parameter['s3_bucket_location']}")

            return True

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger,
                                          inspect.currentframe().f_code.co_name,
                                          err)
            return False

    def save_streaming_multiload(self):

        authentication = {"USER": "", "PASSWORD": ""}
        payload = {"query": "some query"}

        session = requests.Session()
        response = session.post("URL", data=payload, auth=(authentication["USER"], authentication["PASSWORD"]), stream=True)
        s3_bucket = "bucket_name"
        s3_file_path = "path_in_s3"
        s3 = boto3.client('s3')
        with response as part:
            part.raw.decode_content = True
        conf = boto3.s3.transfer.TransferConfig(multipart_threshold=10000, max_concurrency=4)
        s3.upload_fileobj(part.raw, s3_bucket, s3_file_path, Config=conf)

    def load(self, location: str, storage_format: str = None, storage_parameter: dict = None, *args, **kwargs) -> bool:

        try:

            _storage_parameter = self.parse_location(location)
            #_storage_parameter["object_key"] = _storage_parameter["object_key"][0] if _storage_parameter["object_key"] else ""
            ### Object doesn't exist
            if not _storage_parameter:
                return False


            if storage_parameter:
                _storage_parameter = {**_storage_parameter, **storage_parameter}

            if self._storage_parameter:
                # merge location and storage parameter with location take precedence:
                _storage_parameter = {**self._storage_parameter, **_storage_parameter}

            if "s3_bucket_location" not in _storage_parameter:
                pggenericfunc.pg_error_logger(self._logger,
                                              inspect.currentframe().f_code.co_name,
                                              "Storage parameter s3_bucket_location is not set correctly")
                return False

            if "mode" not in _storage_parameter:
                _storage_parameter['mode'] = "direct"

            # if there is storage format attached
            print(f"storage_parameter: {_storage_parameter}")
            return self._load_size_map[f"{_storage_parameter['mode']}_default"](_storage_parameter, storage_format)

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
        return False

    def load_direct_default(self, location: dict, storage_format=None):
        #if not pggenericfunc.pg_errorcheck_exist("object_key", location):

        ### Retrieve Data from S3
        _data = []
        try:
            if "object_key" in location:
                _result = self._s3_client.get_object(Bucket=location['s3_bucket_location'],
                                                     Key=location['object_key'])
                if _result['ResponseMetadata']['HTTPStatusCode'] == 200:
                    _data.append({"object_key": location['object_key'], "StreamingBody": _result['Body']})
                    #print(_data)

                else:
                    pggenericfunc.pg_error_logger(self._logger,
                                                  inspect.currentframe().f_code.co_name,
                                                  f"data retrieval from {pgdirectory.add_splash_2_dir(location['s3_bucket_location']) + location['object_key']}")
                    raise
            else:
                for _object_key in self.list_keys(location['s3_bucket_location']):
                    _result = self._s3_client.get_object(Bucket=location['s3_bucket_location'],
                                                         Key=_object_key)
                    if _result['ResponseMetadata']['HTTPStatusCode'] == 200:
                        #pprint(_result)
                        # _result['Body'].read().decode('utf-8')
                        _data.append({"object_key": _object_key, "StreamingBody": _result['Body']})
                    else:
                        pggenericfunc.pg_error_logger(self._logger,
                                                      inspect.currentframe().f_code.co_name,
                                                      f"Data retrieval from {pgdirectory.add_splash_2_dir(location['s3_bucket_location']) + _object_key}")
                        raise

            _decode_method = storage_format or ("decode_method" in self.storage_parameter and
                                                self.storage_parameter['decode_method'])
            if _decode_method:
                _temp = []
                for item in _data:
                    #self._data.seek(0)
                    _t = item['StreamingBody'].read()
                    #_t.seek(0)
                    print(type(_t))
                    print(io.BytesIO(_t))
                    #pd.read_csv(data)
                    print(pd.read_csv(io.BytesIO(_t), header=None))
            else:
                return _data

                    #print(type(self._storage_format_instance.decode(item['StreamingBody'].read())))
                    #_temp.append(self._storage_format_instance.decode(item['StreamingBody'].read()))
                #print(_temp)
                    #self._storage_format_instance.data['StreamingBody']:
            # df = pd.read_csv(BytesIO(bytes_data))
            """
            if self._storage_format_instance:
                try:
                    if location['object_key']:
                        if not self._storage_format_instance.load_file(
                                pgdirectory.add_splash_2_dir(location['s3_bucket_location']) + _location[
                                    'object_key']):
                            return False
                    else:
                        for _object_key in self.list_keys(_location['s3_bucket_location']):
                            if not self._storage_format_instance.load_file(
                                    pgdirectory.add_splash_2_dir(_location['s3_bucket_location']) + _object_key):
                                return False
                    return True

                except Exception as err:
                    if self._logger:
                        self._logger.Critical(err)
                    else:
                        print(err)
                    return False
            """

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return False

    def load_file_default(self, location: dict, storage_format=None) -> bool:
        """Returns True if file(s) are successfully downloaded from specified s3 location.

        Args:
            location:       location parameter, includes s3_bucket_location, directory (directory which files will land)
                            object_key (specific filepath in s3) or entire S3 bucket if not specified
            storage_format: A storage format instance

        Returns:
            The return value. True for success, False otherwise.

        """

        try:
            _location = {**self._storage_parameter, **location} if self._storage_parameter else location
            pprint(_location)

            check_args(inspect.currentframe().f_code.co_name,
                       {'s3_bucket_location': _location['s3_bucket_location'] if "s3_bucket_location" in _location else None,
                        'directory': _location['directory'] if "directory" in _location else None,
                        }, False)

            pgdirectory.isdirectoryexist(_location['directory'], False)

            if "object_key" in _location and _location['object_key']:
                if isinstance(_location['object_key'], str):
                    _location['object_key'] = [_location['object_key']]

                if isinstance(_location['object_key'], list):
                    _location['object_key'] = [x.replace("s3://", "").split('/')[1:] if x.startswith("s3://") else x for x in _location['object_key']]
                    #print(_location['object_key'])
                    for _pg_each_file in _location['object_key']:
                        self._s3_client.download_file(_location['s3_bucket_location'],
                                                      _pg_each_file,
                                                      pgdirectory.add_splash_2_dir(_location['directory']) + _pg_each_file.split('/')[-1])
                #elif isinstance(_location['object_key'], str):
                #    _location['object_key'] = _location['object_key'].replace("s3://", "").split('/')[1:]
            else:
                for _object_key in self.list_keys(location['s3_bucket_location']):
                    self._s3_client.download_file(_location['s3_bucket_location'], _object_key,
                                                  pgdirectory.add_splash_2_dir(_location['directory']) + _object_key)
            return True
            #_location['object_key'] = '/'.join(_location['object_key'].replace("s3://", "").split('/')[1:]) if \
            #    _location['object_key'].startswith("s3://") else _location['object_key']
            """
            if "object_key" in _location:
                _filename = _location['object_key'].split('/')[-1]
                self._s3_client.download_file(_location['s3_bucket_location'], _location['object_key'],
                                              pgdirectory.add_splash_2_dir(_location['directory']) + _filename)
            else:
                for _object_key in self.list_keys(location['s3_bucket_location']):
                    self._s3_client.download_file(_location['s3_bucket_location'], _object_key,
                                                  pgdirectory.add_splash_2_dir(_location['directory']) + _object_key)
            return True
            """
        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return False

        #self._s3_client.download_file('your_bucket', 'k.png', '/Users/username/Desktop/k.png')
        """
        pprint(self._data)
        for _data in self._data:
            with open(f"{pgdirectory.add_splash_2_dir(_location['directory'])} + {_data['object_key']}", 'w') as file:
                for lines in _data['StreamingBody'].read().decode('utf-8').split('\n'):
                    file.write(lines)
        return True
        """

    def load_streaming_multiload(self):
        pggenericfunc.notimplemented()


    def list_bucket(self) -> list:
        try:
            return [f"{bucket['Name']}" for bucket in self._s3_client.list_buckets()['Buckets']]
        except ClientError as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return []

    @pgoperation.pg_retry(3)
    def list_keys(self, bucket: str) -> list:
        try:
            _result = self._s3_client.list_objects(Bucket=bucket)
            pprint(_result)
            if _result and _result['ResponseMetadata']['HTTPStatusCode'] == 200:
                if "Contents" in _result:
                    for _key in self._s3_client.list_objects(Bucket=bucket)['Contents']:
                        yield(_key['Key'])
            return []
        except ClientError as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return []

    def create_s3_signed_url(self, method: str, bucket_name: str, key: str, expiration: str) -> object:
        try:
            return self._s3_client.generate_presigned_url(ClientMethod=method,
                                                          Params={'Bucket': bucket_name,
                                                                  'Key': key},
                                                          ExpiresIn=expiration)
        except ClientError as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return None

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return ""

    #def __str__(self): return
    #__repr__ = __str__

"""
Untested
"""


class S3File(io.RawIOBase):
    def __init__(self, s3_object):
        self.s3_object = s3_object
        self.position = 0

    def __repr__(self):
        return "<%s s3_object=%r>" % (type(self).__name__, self.s3_object)

    @property
    def size(self):
        return self.s3_object.content_length

    def tell(self):
        return self.position

    def seek(self, offset, whence=io.SEEK_SET):
        if whence == io.SEEK_SET:
            self.position = offset
        elif whence == io.SEEK_CUR:
            self.position += offset
        elif whence == io.SEEK_END:
            self.position = self.size + offset
        else:
            raise ValueError("invalid whence (%r, should be %d, %d, %d)" % (
                whence, io.SEEK_SET, io.SEEK_CUR, io.SEEK_END
            ))

        return self.position

    def seekable(self):
        return True

    def read(self, size=-1):
        if size == -1:
            # Read to the end of the file
            range_header = "bytes=%d-" % self.position
            self.seek(offset=0, whence=io.SEEK_END)
        else:
            new_position = self.position + size

            # If we're going to read beyond the end of the object, return
            # the entire object.
            if new_position >= self.size:
                return self.read()

            range_header = "bytes=%d-%d" % (self.position, new_position - 1)
            self.seek(offset=size, whence=io.SEEK_CUR)

        return self.s3_object.get(Range=range_header)["Body"].read()

    def readable(self):
        return True


class PGS3Ext(PGS3):
    def __init__(self, project_name: str = "pgs3ext", logging_enable: str = False):
        super(PGS3Ext, self).__init__(project_name=project_name, logging_enable=logging_enable)

        ### Specific Variables
        self._model_subtype = {}
        os.environ['KMP_DUPLICATE_LIB_OK'] = 'True'

    def get_tasks(self, pg_data: str, pg_parameters: dict = None) -> bool:

        """Prepares inputs into a standard format
        [(name(string), parameters(dict), data(varies), (name1(string), parameters1(dict), data1(varies), ...]

        Three type of inputs:
        1) if dataset is a string, then expects it to be a filename in yaml format
        2) if dataset is a list with no parameter, then expects to be the final format above
        3) if dataset is a list with parameter, then it will convert it to be the final format above

        Args:
            pg_data: full url of s3 bucket name and key  # eg. s3://tag-data-app-prod-0001/07_18_2021/
            pg_parameters: parameters

        Returns:

        """
        try:
            self.create_client(aws_access_key_id=pg_parameters["aws_access_key_id"],
                               aws_secret_access_key=pg_parameters["aws_secret_access_key"],
                               region_name=pg_parameters["region_name"])

            _task_list = {"0": self._s3_client.copy_object,
                          "1": self._s3_client.delete_object,
                          "2": self._s3_client.list_objects_v2
                          }

            #print(pg_parameters["aws_access_key_id"])
            _pg_parse = self.parse_location(pg_data)
            if not _pg_parse:
                return True

            _response = None

            print(_pg_parse)
            for item in _pg_parse["object_key"]:
                if not item.startswith("PENDING_"):
                    print(f"PENDING_{'/'.join(item.split('/')[:-1])}/{item.split('/')[-1]}")
                    #_pg_target_key = f"PENDING_{'/'.join(item.split('/')[:-1])}/{item.split('/')[-1]}"

                    for _pg_step, _pg_action in _task_list.items():
                        if _pg_step == "0":
                            _response = _pg_action(Bucket=_pg_parse['s3_bucket_location'],
                                                   CopySource=f"{_pg_parse['s3_bucket_location']}/{item}",
                                                   Key=f"PENDING_{'/'.join(item.split('/')[:-1])}/{item.split('/')[-1]}",
                                                   ServerSideEncryption="AES256")

                        elif _pg_step == "1":
                            _response = _pg_action(Bucket=_pg_parse['s3_bucket_location'], Key=item)

                        elif _pg_step == "2":
                            _response = _pg_action(Bucket=_pg_parse['s3_bucket_location'], Prefix=f"PENDING_{item}")
                            if self._data_inputs:
                                self._data_inputs[pg_parameters['entity_name']]['data'] = self._data_inputs[pg_parameters['entity_name']]['data'] + [f"s3://{_pg_parse['s3_bucket_location']}/{x['Key']}" for x in _response['Contents']]
                            else:
                                self._data_inputs = {pg_parameters['entity_name']: {
                                    'data': [f"s3://{_pg_parse['s3_bucket_location']}/{x['Key']}" for x in _response['Contents']],
                                    'parameter': pg_parameters
                                }
                                }
                        if _response and _response["ResponseMetadata"]["HTTPStatusCode"] >= 300:
                            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name,
                                                          f"Error, Step {_pg_step} encountered an error")
                            return False


        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
        return False

    def _process(self, pg_data_name: str, pg_data=None, pg_parameters: dict = {}) -> Union[float, int, None]:
        try:
            #print(pg_data)
            #exit(0)
            if isinstance(pg_data, str):
                pg_data = [pg_data]
            if isinstance(pg_data, list):
                for _pg_data_item in pg_data:
                    self.load(_pg_data_item, storage_parameter=pg_parameters)


            """
            if isinstance(pg_data, pd.DataFrame):
                self._data[pg_data_name] = None
            elif isinstance(pg_data, str):
                self.load(pg_data, storage_parameter=pg_parameters)
                with pgstorageformat.pg_set_storage_format(pg_data) as pg_file_format:
                    return pg_file_format
            elif isinstance(pg_data, list):
                self._data[pg_data_name] = []
                for _pg_data_item in pg_data:
                    self.load(_pg_data_item, storage_parameter=pg_parameters)
                    print(f"{pg_parameters['directory']}/{_pg_data_item.split('/')[-1]}")
                    with pgstorageformat.pg_set_storage_format(f"{pg_parameters['directory']}/{_pg_data_item.split('/')[-1]}") as pg_file_format:
                        self._data[pg_data_name].append(pg_file_format)
            return self._data[pg_data_name]
            """
        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
        return None

    def process(self, name: str = None, *args: object, **kwargs: object) -> bool:
        try:
            if name and self._data_inputs:
                _item = self._data_inputs[name]
                self._data[name] = self._process(name, _item['data'], _item['parameter'], )
            else:
                for _index, _data in self._data_inputs.items():
                    _item = self._data_inputs[_index]
                    self._data[_index] = self._process(name, _item['data'], _item['parameter'])
            return True

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return False


if __name__ == '__main__':
    test = PGS3Ext()

    test_parameter = {"directory": "/Users/jianhuang/opt/anaconda3/envs/Data26/Data26/Learning/PyCaret/Test2",
                      "aws_access_key_id": "AKIAXW42WHCP3KKG2BPJ",
                      "aws_secret_access_key": "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",
                      "region_name": "us-east-1",
                      "mode": "file",
                      "entity_name": "test"}
    #test.create_client(aws_access_key_id=test_parameter["aws_access_key_id"],
    #                   aws_secret_access_key=test_parameter["aws_secret_access_key"],
    #                   region_name=test_parameter["region_name"])

    #test.load('s3://tag-data-app-prod-0001/07_18_2021_PENDING/output.csv', storage_parameter=test_parameter)
    #exit(0)

    #/07_18_2021/output.csv
    #test.get_tasks("s3://tag-data-app-prod-0001/07_18_2021/", test_parameter)
    test.get_tasks("s3://tag-data-app-prod-0001/", test_parameter)
    print(f"data input: {test._data_inputs}")
    test.process("test", test_parameter)

    print(f"data: {test._data}")
    exit(0)
    mys3 = PGS3()

    mys3.create_client()
    print(mys3.list_bucket())

    mys3.set_param(storage_parameter={'s3_bucket_location': "aaaaa",
                                      #'object_key': "",
                                      'mode': "file",
                                      'directory': "/Users/jianhuang/opt/anaconda3/envs/Data20/Data20/Data/Storage/S3/Test/temp",
                                      #'filename': "ccccc"
    })
    #print(f"{pgdirectory.add_splash_2_dir(mys3._storage_parameter['s3_bucket_location']) + mys3._storage_parameter['object_key']}")
    for key in mys3.s3_client.list_objects(Bucket='testba8a4bda')['Contents']:   # again assumes boto.cfg setup, assume AWS S3
        print(key['Key'])

    print(mys3.parse_location("testba8a4bda/file_0f457328"))

    mys3.load("s3://testba8a4bda")
    pprint(mys3._data)

    #print(mys3.list_bucket())

    exit(0)
    random_string = pgsecret.generate_secret(16)
    random_file = f"/Users/jianhuang/test100/New/temp/file_{random_string}"
    with open(random_file, 'wb') as fout:
        fout.write(os.urandom(1024))

    #print(mys3._config.parameters["config_file"]['default']['aws_access_key_id'])
    #print(mys3._config.parameters["config_file"]['default']['aws_secret_access_key'])


    mys3.create_bucket(f"test{random_string}")

    mys3.set_param(storage_parameter={'s3_bucket_location': f"test{random_string}",
                                      'object_key': f"file_{random_string}",
                                      'directory': "/Users/jianhuang/test100/New/temp",
                                      'filename': f"file_{random_string}",
                                      }
                   )
    mys3.save(f"/Users/jianhuang/test100/New/temp/file_{random_string}")


    # AWSs3().create_bucket("test", region='us-east-2')