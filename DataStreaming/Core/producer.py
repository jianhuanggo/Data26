from confluent_kafka import Producer
import sys
import pandas as pd


class KafkaProducer:

    def __init__(self, args):
        self.args = args
        self.kproducer = Producer(**self.args.conf)
        self.count = 0

    def delivery_callback(self, err, msg):
        if err:
            sys.stderr.write('%% Message failed delivery: %s\n' % err)
        else:
            sys.stderr.write('%% Message delivered to %s [%d] @ %o\n' %
                            (msg.topic(), msg.partition(), msg.offset()))

    def run(self):

        try:
            input_df = pd.read_csv('/Users/jianhuang/scoot_data/data/data_mover/invoices/save2113f6_invoices.csv', index_col=False, header=None)

            for row in input_df.itertuples(index=True, name='Pandas'):
                print(len(row))
                # getattr(row, "c1"), getattr(row, "c2")
                self.kproducer.produce(self.args.topic, str(len(row)), callback=self.delivery_callback)
                self.count += 1
                if self.count > 10:
                     break


        except BufferError:
            sys.stderr.write('%% Local producer queue is full (%d messages awaiting delivery): try again\n' %
                             len(self.kproducer))

        """
        for line in sys.stdin:
            try:

                self.kproducer.produce(self.args.topic, line.rstrip(), callback=self.delivery_callback)

            except BufferError:
                sys.stderr.write('%% Local producer queue is full (%d messages awaiting delivery): try again\n' %
                                 len(self.kproducer))

            self.kproducer.poll(0)
        """

        sys.stderr.write('%% Waiting for %d deliveries\n' % len(self.kproducer))
        self.kproducer.flush()


