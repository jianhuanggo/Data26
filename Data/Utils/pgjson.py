import json
import inspect
from Meta import pggenericfunc, pgclassdefault

def pg_serialize_to_disk(pg_data: dict, pg_filepath: str, logger=None) -> bool:
    try:
        with open(pg_filepath, "w") as json_file:
            json.dump(pg_data, json_file)
        return True
    except Exception as err:
        pggenericfunc.pg_error_logger(logger, inspect.currentframe().f_code.co_name, err)
    return False

def pg_deserialize_from_disk(pg_filepath: str, logger=None):
    try:
        with open(pg_filepath, "r") as json_file:
            #_content = json_file.read()
            return json.loads(json_file.read().replace("][", ","))
            # else:
            #     return json.load(json_file)
    except Exception as err:
        pggenericfunc.pg_error_logger(logger, inspect.currentframe().f_code.co_name, err)
    return None

if __name__ == "__main__":
    pass