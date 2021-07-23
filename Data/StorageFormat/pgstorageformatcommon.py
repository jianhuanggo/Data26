from Meta import pgclassdefault


class PGStorageFormatCommon(pgclassdefault.PGClassDefault):
    def __init__(self, project_name: str,
                 object_short_name: str,
                 config_file_pathname: str,
                 logging_enable: str,
                 config_file_type: str):

        super().__init__(project_name=project_name,
                         object_short_name=object_short_name,
                         config_file_pathname=config_file_pathname,
                         logging_enable=logging_enable,
                         config_file_type=config_file_type)
