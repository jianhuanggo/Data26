from DataStreaming.Core import cli
from DataStreaming.Core import consumer

if __name__ == '__main__':
    args = cli.get_parser_consumer()
    consumer.KafkaConsumer(args).run()
