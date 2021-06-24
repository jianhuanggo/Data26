
class JobManagement:
    def __init__(self, args):
        self._postgresql_host = None
        self._postgresql_username = None
        self._postgresql_password = None
        self._post_port = None
        self._postgresql_database = None
        self._postgresql_url = None


        self._redshift_host = None
        self._redshift_user = None
        self._redshift_pass = None
        self._redshift_db = None
        self._redshift_port = None
        self._redshift_url = None
        self._target_lookup = None
        self.hwm_target = None
        self._query_result = None


    def setup_postgresql(self):

        self._postgresql_host = getattr(self.conf, 'SCOOT_RDS_POST_HOST')
        self._postgresql_username = getattr(self.conf, 'SCOOT_RDS_POST_USERNAME')
        self._postgresql_password = getattr(self.conf, 'SCOOT_RDS_POST_PASS')
        self._post_port = getattr(self.conf, 'SCOOT_RDS_POST_PORT')
        self._postgresql_database = getattr(self.conf, 'SCOOT_RDS_POST_DB')
        self._postgresql_url = f"postgresql://{self._postgresql_username}:" \
                               f"{self._postgresql_password}@{self._postgresql_host}/{self._postgresql_database}"
        self._target_lookup = {'source': self._postgresql_url}
        self.args.logger.info("Sucessfully initiated db connection to RDS...")

    def setup_redshift(self):

        self._redshift_host = getattr(self.conf, 'SCOOT_REDSHIFT_HOST')
        self._redshift_user = getattr(self.conf, 'SCOOT_REDSHIFT_USERNAME')
        self._redshift_pass = getattr(self.conf, 'SCOOT_REDSHIFT_PASS')
        self._redshift_db = getattr(self.conf, 'SCOOT_REDSHIFT_DB')
        self._redshift_port = getattr(self.conf, 'SCOOT_REDSHIFT_PORT')
        self._redshift_url = f"postgresql://{self._redshift_user}:{self._redshift_pass}@{self._redshift_host}:{self._redshift_port}/{self._redshift_db}"
        self._target_lookup = {'stage': self._redshift_url,
                               'target': self._redshift_url}
        self.args.logger.info("Sucessfully initiated db connection to Data Warehouse...")


