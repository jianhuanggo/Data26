import inspect
from Meta import pggenericfunc, pgclassdefault


class PGValidateCommon(pgclassdefault.PGClassDefault):
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

    def parse_response(self, response_text: str) -> bool:
        responses = (("something about your browser made us think you might be a bot", False),
                     ("You're a power user moving through this website with super-human speed", False)
                     )

        #success_responses = {'0': " "}

        if not response_text:

            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, "response text is empty, nothing to process")
            return False

        try:
            print(response_text)
            for response in responses:
                if response[0] in response_text:
                    return response[1]

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
        return True

