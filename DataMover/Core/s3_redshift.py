import logging
import argparse
import sys
from types import SimpleNamespace
from Data.Utils import pgyaml
from Data.Connect import redshift
from Data.Config import pgconfig
from Data.Utils import pgprocess

_version_ = 0.1


def get_parser():
    try:
        logging.debug('defining argparse arguments')

        argparser = argparse.ArgumentParser(description='S3 to Redshift Mover')
        argparser.add_argument('-v', '--version', action='version', version='%(prog)s VERSION ' + str(_version_),
                               help='show current version')
        argparser.add_argument('-f', '--yaml_file', action='store', dest='yaml_file', required=True,
                               help='Yaml file')
        logging.debug('parsing argparser arguments')
        args = argparser.parse_args()

    except Exception as err:
        logging.critical("Error creating/parsing arguments:\n%s" % str(err))
        sys.exit(100)

    # print(args.daemon_type)
    return args


class s3_to_redshift:

    def __init__(self, args=None):
        self.args = args
        self.yaml_content = pgyaml.yaml_load(yaml_filename=args.yaml_file)
        print(self.yaml_content)
        self.conf = pgconfig.Config().setup_redshift()
        self.parameter = pgconfig.Config().parameters

    def move(self):
        fn = f"s3://{self.yaml_content['s3_location']}"
        print(fn)
        quote = ''

        opt = SimpleNamespace()

        opt.red_quote = 0
        opt.red_ignoreheader = 0
        opt.red_timeformat = 0
        opt.red_to_table = self.yaml_content['table_name']

        opt.red_col_delim = ','

        if opt.red_quote:
            quote = 'quote \'%s\'' % opt.red_quote
        ignoreheader = ''
        if opt.red_ignoreheader:
            ignoreheader = 'IGNOREHEADER %s' % opt.red_ignoreheader
        timeformat = ''
        if opt.red_timeformat:
            # timeformat=" dateformat 'auto' "
            timeformat = " TIMEFORMAT '%s'" % opt.red_timeformat.strip().strip("'")

        #redshift_access_key = self.yaml_content['s3_bucket_access_key']
        #redshift_secret_key = self.yaml_content['s3_bucket_secret_key']

        if bool(self.yaml_content['gzip']):
            set_gzip = "GZIP"
        else:
            set_gzip = ""

        red = redshift.ConnectRedshift(self.conf)

        column_list = red.get_column_list_by_table(table_catalog=self.yaml_content['redshift_table_catalog'],
                                                   schema_name=self.yaml_content['redshift_schema'],
                                                   table_name=self.yaml_content['table_name']).fetchall()

        print(column_list)
        column_list.sort(key=lambda x: x[1])

        arg_column_list_str = "(" + ','.join(["\"" + item[0] + "\"" for item in column_list]) + ")"

        sql = """
                COPY %s %s FROM '%s' 
                	iam_role 'arn:aws:iam::085201521026:role/RedShiftS3'
                	DELIMITER '%s' 
                	FORMAT CSV %s 
                	%s
                	%s 
                	%s COMPUPDATE OFF STATUPDATE OFF; 
                	COMMIT;
                """ % (
        opt.red_to_table, arg_column_list_str, fn, opt.red_col_delim, set_gzip, quote, timeformat, ignoreheader)
        print(sql)
        try:
            pgprocess.run_query(db_client_dbshell=self.parameter['SCOOT_CLIENT_DBSHELL'].strip(),
                                db_url=red.url,
                                query=sql)

        except Exception as err:
            self.args.logger.critical(f"Error in connecting Redshift {err}")
            raise ("Error in loadding data to Redshift...")
        else:
            self.args.logger.info(f"Data has been loaded to {opt.red_to_table} successfully!")
            return True

        return False

if __name__ == '__main__':
    args = get_parser()
    s3_to_redshift(args).move()

