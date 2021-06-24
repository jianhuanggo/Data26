import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
import csv
from collections import namedtuple


FORECAST = "forecast.csv"
fields = ("field1", "field2", "field3", "field4", "field5", "field6", "field7", "field8", "field9", "field10", "field11", "field12")
forecastRecord = namedtuple('forecastRecord', fields)


def read_forecast_data(path):
    with open(path, 'rU') as data:
        data.readline()
        reader = csv.reader(data, delimiter = ";")
        for row in map(forecastRecord._make, reader):
            print(row)
            yield row

if __name__=="__main__":
    for row in read_forecast_data(FORECAST):
        print (row)
        break