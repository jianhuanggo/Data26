from Data.Utils import pgfile

class Aws_s3:

    def __init__(self, location=None, filename=None):
        if not location:
            self._s3_bucket_location = 's3.us-west-1.amazonaws.com/data-pipeline-4/'
        else:
            self._s3_bucket_location = location

        if not filename:
            self._s3_filename = pgfile.get_random_filename("autogen")
        else:
            self._s3_filename = filename

