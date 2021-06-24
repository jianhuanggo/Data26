import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
import csv
from collections import namedtuple
import io
import json


FORECAST = "forecast.csv"
fields = ("field1", "field2", "field3", "field4", "field5", "field6", "field7", "field8", "field9", "field10", "field11", "field12")
forecastRecord = namedtuple('forecastRecord', fields)


def read_data(*, filepath: str, tablestruct):
    with open(filepath, 'r') as data:
        data.readline()
        reader = csv.reader(data, delimiter=";")
        print(reader)
        for row in map(tablestruct._make, reader):
            print(row)
            yield row


def parse_schema(path="/Users/jianhuang/PycharmProjects/Data/Data/Utils/test.avsc"):
    print("ok")
    with open(path, 'r') as data:
        print(data.read())
        return avro.schema.Parse(json.dumps(data.read()))


def serialize_records(records, outpath="forecast.avro"):
    schema = parse_schema()
    with open(outpath, 'w') as out:
        writer = DataFileWriter(out, DatumWriter(), schema)
        for record in records:
            record = dict((f, getattr(record, f)) for f in record._fields)
            writer.append(record)


def serialize_records_v2(records):
    buf = io.BytesIO()
    schema = parse_schema()
    print(schema)
    exit(0)
    writer = DataFileWriter(buf, DatumWriter(), schema)
    for record in records:
        record = dict((f, getattr(record, f)) for f in record._fields)
        writer.append(record)
    return buf


def send_message(connection, message):
    buf = io.BytesIO()
    writer = avro.datafile.DataFileWriter(buf, avro.io.DatumWriter(), SCHEMA)
    writer.append(message)
    writer.flush()
    buf.seek(0)
    data = buf.read()
    connection.send(data)


if __name__ == "__main__":
    for row in read_data(filepath=FORECAST, tablestruct=forecastRecord):
        print(row)
        break
    serialize_records_v2(read_data(filepath=FORECAST, tablestruct=forecastRecord))
