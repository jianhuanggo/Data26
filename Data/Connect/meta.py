from Data.Connect import postgresql
from Data.Connect import redshift
from Data.Connect import s3


def meta_lookup(system):

    connector = {'postgresql': postgresql.ConnectPostgresql,
                 'redshift': redshift.ConnectRedshift,
                 's3': s3.Aws_s3,
                }

    if system not in connector:
        return -1

    return connector[system]
