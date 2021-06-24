import uncurl
import re
import requests
import Data.Utils.pgutilsbase as pgutilsbase
import Data.Utils.pgyaml as pgyl
import Data.Utils.pgfile as pgfile
import Data.Utils.pgdirectory as pgdirectory


class PGcurl2python(pgutilsbase.PGUtilsBase):
    def __init__(self):
        super().__init__()
        self._config.parameters = pgyl.yaml_load(yaml_filename=pgdirectory.currentdirectory() + "/" + self.__class__.__name__.lower() + ".yml")
        # Regex to match a single backslash: \\
        # String to describe this regex: "\\\\".
        self._format_operation = {"\\\\\n": "\n",
                                  "--data-raw": "--data"}

    def curl_formatter(self, curl_string: str) -> str:
        """

        """
        #print(list(map(lambda x: re.sub(x, format_operation[x], curl_string), format_operation))[-1])
        for search_string, replace_string in self._format_operation.items():
            curl_string = re.sub(search_string, replace_string, curl_string.rstrip())
        return curl_string

    def curl_2_python(self, curl_string: str):
        return uncurl.parse(self.curl_formatter(curl_string))

    def add_formatter(self, custom_transform: dict):
        self._format_operation = {**self._format_operation, **dict(custom_transform)}

    def write_file(self, curl_string: str, prefix, file_extension, post_script_content=None, path=None, filename=None):
        """
        if filename is provided, it will use as it is.
        if filename is not provided, then it will generate a filename with format of
        <prefix><random string>.<file_extension>
        """
        if filename is None:
            filename = pgfile.get_random_filename(prefix) + "." + file_extension
        if path and path[-1] != '/':
            path += '/'
        print(path)
        curl_string = f"import requests\n\nx={curl_string}\n\n{post_script_content}"
        with open(f"{path}{filename}", 'w') as file:
            file.write(curl_string)






if __name__ == '__main__':
    x = PGcurl2python()
    latitude_coord = 41.7888
    longitude_coord = -88.000
    a = {"41.7976163": str(latitude_coord), "-87.9775787": str(longitude_coord)}
    x.add_formatter(a)
    with open('/Users/jianhuang/opt/anaconda3/envs/Data20/Data20/test.curl') as file:
        x.write_file(x.curl_2_python(file.read()), "covid_vac_walgreen", "py", "print(x.text)")
        #print(uncurl.parse(x.curl_formatter(file.read())))


