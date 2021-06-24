from Data.Config import pgconfig


class ConStrMixin:
    def get_postgresql_url(self):
        conf = pgconfig.Config()
        return f"postgresql://{getattr(conf, 'SCOOT_RDS_POST_USERNAME')}:" \
               f"{getattr(conf, 'SCOOT_RDS_POST_PASS')}@{getattr(conf, 'SCOOT_RDS_POST_HOST')}:" \
               f"{getattr(conf, 'SCOOT_RDS_POST_PORT')}/{getattr(conf, 'SCOOT_RDS_POST_DB')}"

    def setup_redshift(self):
        conf = pgconfig.Config()

        return f"postgresql://{getattr(conf, 'SCOOT_REDSHIFT_USERNAME')}:" \
               f"{getattr(conf, 'SCOOT_REDSHIFT_PASS')}@{getattr(conf, 'SCOOT_REDSHIFT_HOST')}:" \
               f"{getattr(conf, 'SCOOT_REDSHIFT_PORT')}/{getattr(conf, 'SCOOT_REDSHIFT_DB')}"


if __name__ == '__main__':
    print(ConStrMixin().get_postgresql_url())
    print(ConStrMixin().setup_redshift())
