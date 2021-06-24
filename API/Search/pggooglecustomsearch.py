"""
Sign up google api key:  console.cloud.google.com  -> API Library
Sign up for custom search engine cx key:
     google custom search engine cx key, go to "Search engine ID - Programmable Search Engine Help"
     <script async src="https://cse.google.com/cse.js?cx=22b1b136d8f6b480f"></script>
     <div class="gcse-search"></div>

"""

import json
import Data.Utils.pgdirectory as pgdirectory
import Data.Logging.pglogging as pglogging
import Meta.pgclassdefault as pgclassdefault
import requests

__VERSION__ = "2.0"

LOGGING_LEVEL = pglogging.logging.INFO
pglogging.logging.basicConfig(level=LOGGING_LEVEL)


class PGGoogleCustomSearch(pgclassdefault.PGClassDefault):
    def __init__(self, project_name, logging_enable=False):
        super(PGGoogleCustomSearch, self).__init__(project_name=project_name,
                                                   object_short_name="PG_GCS",
                                                   config_file_pathname=pgdirectory.filedirectory(__file__) + "/" + self.__class__.__name__.lower() + ".yml",
                                                   logging_enable=logging_enable)

        self._baseurl = "https://customsearch.googleapis.com/customsearch/v1"
        self._oldbaseurl = "https://www.googleapis.com/customsearch/v1"
        self._last_result = None

    def search(self, question, search_item, search_type="orTerms"):
        parameters = {"q": question, "cx": self.config.parameters['cx'], "key": self.config.parameters['api_key'],
                      "filter": "1", search_type: search_item}

        try:
            self._last_result = json.loads(requests.request("GET", self._baseurl, params=parameters).text)
            return self._last_result
        except Exception as err:
            raise err

    def print_item(self):
        if self._last_result:
            try:
                for item in self._last_result['items']:
                    print(item['snippet'])
            except Exception as err:
                if not self._logger:
                    raise Exception(f"{err}\nSomething is wrong! Not able to get search result")
                else:
                    self._logger(f"{err}\nSomething is wrong! Not able to get search result")


if __name__ == '__main__':
    test = PGGoogleCustomSearch("test")
    test.config.print_all_parameters()
    test.search("the program to re-visit the moon in 2024 is named for what greek god", "persephone phoebe")
    test.print_item()



