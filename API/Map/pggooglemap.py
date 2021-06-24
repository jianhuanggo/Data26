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
import googlemaps
from pprint import pprint
import requests

__VERSION__ = "2.0"

LOGGING_LEVEL = pglogging.logging.INFO
pglogging.logging.basicConfig(level=LOGGING_LEVEL)


class PGGoogleMap(pgclassdefault.PGClassDefault):
    def __init__(self, project_name, logging_enable=False):
        super(PGGoogleMap, self).__init__(project_name=project_name,
                                          object_short_name="PG_GMAP",
                                          config_file_pathname=pgdirectory.filedirectory(__file__) + "/" + self.__class__.__name__.lower() + ".yml",
                                          logging_enable=logging_enable)

        print(self.config.parameters['api_key'])
        self._client = googlemaps.Client(self.config.parameters['api_key'])
        print(sorted(dir(self._client)))

    def get_geocode(self, address: str) -> dict:
        return self._client.geocode(address)[0]['geometry']['location']

    def get_distance(self, origin_geo: tuple, destination_geo: tuple, mode="driving") -> tuple:
        """
        Available modes are: "driving", "walking", "transit" or "bicycling"
        default mode to "driving"
        """
        try:
            response = self._client.distance_matrix(origin_geo, destination_geo, mode=mode)['rows'][0]['elements'][0]
            return (response['distance']['value'], response['duration']['value']) if response['status'] == "OK" else ()
        except Exception as err:
            if not self._logger:
                raise Exception(f"{err}\nSomething is wrong! Not able to get distance")
            else:
                self._logger(f"{err}\nSomething is wrong! Not able to get distance")


if __name__ == '__main__':
    test = PGGoogleMap("googlemaptest")
    pprint(test.get_distance(tuple(test.get_geocode("1405 hornell ln. 30082").values()),
                             tuple(test.get_geocode("North Ave NW, Atlanta, GA 30332").values())))

