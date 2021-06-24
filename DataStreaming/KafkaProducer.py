from DataStreaming.Core import cli
from DataStreaming.Core import producer

if __name__ == '__main__':
    args = cli.get_parser_producer()
    producer.KafkaProducer(args).run()
