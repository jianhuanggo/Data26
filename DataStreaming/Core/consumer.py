from confluent_kafka import Consumer, KafkaException
import logging
import sys

class KafkaConsumer:

    def __init__(self, args):
        self.logger = logging.getLogger('consumer')
        self.logger.setLevel(logging.DEBUG)
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter('%(asctime)-15s %(levelname)-8s %(message)s'))
        self.logger.addHandler(handler)

        self.Kconsumer = Consumer(args.conf, logger=self.logger)
        self.args = args

    def print_assignment(self, consumer, partitions):
        print('Assignment:', partitions)

    def run(self):

        self.Kconsumer.subscribe(self.args.topics, on_assign=self.print_assignment)

        try:
            while True:
                msg = self.Kconsumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    raise KafkaException(msg.error())
                else:
                    sys.stderr.write('%% %s [%d] at offset %d with key %s:\n' %
                                     (msg.topic(), msg.partition(), msg.offset(),
                                      str(msg.key())))
                    print(msg.value())

        except KeyboardInterrupt:
            sys.stderr.write('%% Aborted by user\n')

        finally:
            self.Kconsumer.close()

