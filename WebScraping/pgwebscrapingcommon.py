import os
import inspect
from Meta import pggenericfunc, pgclassdefault
from WebScraping import scrapyitem_pb2, scrapyitem_test_pb2


class PGSCrapyItem:
    def __init__(self):
        self._pre_filter = []
        self._url = None
        self._post_filter = []

    def add_filter(self, filter_type: str, filter_string: dict):
        pass


class PGWebScrapingCommon(pgclassdefault.PGClassDefault):
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

        self._item = scrapyitem_pb2.PGScrapyItem()
        self._item_test = scrapyitem_test_pb2.PGScrapyItem_test()

    def seralize_item(self, filepath: str, pg_item=None) -> bool:

        _pg_item = pg_item or self._item
        if not _pg_item: return False

        try:
            print(type(_pg_item))
            with open(filepath, "wb") as file:
                file.write(_pg_item.SerializeToString())
            return True
        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return False

    def deseralize_item(self, filepath: str) -> bool:
        if not filepath: return False
        try:
            with open(filepath, "rb") as file:
                self._item.ParseFromString(file.read())
            return True
        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return False


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

