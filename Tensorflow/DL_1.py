
import tensorflow as tf
import numpy as np
import logging
import argparse
import sys

_version_ = 0.5


def get_parser():
    try:
        logging.debug('defining argparse arguments')

        argparser = argparse.ArgumentParser(description='Simple one variable linear function ML to predict coefficient '
                                                        'and constant given X as a variable and Y as final output')
        argparser.add_argument('-v', '--version', action='version', version='%(prog)s VERSION ' + str(_version_),
                               help='show current version')
        argparser.add_argument('-a', '--a_var', action='store', dest='a_var', required=True, type=int,
                               help='a variable as coefficient of X')
        argparser.add_argument('-b', '--b_var', action='store', dest='b_var', required=True, type=int,
                               help='b variable as constant of the function')

        logging.debug('parsing argparser arguments')
        args = argparser.parse_args()

    except Exception as err:
        logging.critical("Error creating/parsing arguments:\n%s" % str(err))
        sys.exit(100)

    # print(args.daemon_type)
    return args

class DataGeneration:
    def __init__(self, a: int, b: int):
        self._input = None
        self._output = None
        self.a = a
        self.b = b

    def data_gen_sample_input(self):
        sample_input_data = np.random.rand(100).astype(np.float32)

        #pd.DataFrame(sample_input_data).to_csv("sample_input.csv")
        self._input = np.random.rand(100).astype(np.float32)
        #a = 50
        #b = 40
        self._output = self.a * self._input + self.b
        self._output = np.vectorize(lambda y: y + np.random.normal(loc=0.0, scale=0.05))(self._output)
        #pd.DataFrame(Y).to_csv("sample_output.csv")
        #print(f"here is the generated input: {}")
        #print(self._output)

    @property
    def input(self):
        return self._input

    @property
    def output(self):
        return self._output


class DataPrepare:
    def __init__(self):
        self.input_data = None
        self.output_data = None

    def sample_data_prepare(self, input_data, output_data):
        return input_data, output_data

    def data_load_from_csv(self):
        #sample_data = np.random.rand(100).astype(np.float32)
        self.input_data = np.genfromtxt('sample_input.csv', delimiter=',')
        self.output_data = np.genfromtxt('sample_output.csv', delimiter=',')
        #print(self.input_data)
        #print(self.output_data)
        return self.input_data, self.output_data


class TrainModel:
    def __init__(self, X, Y):
        self.X = X
        self.Y = Y
        self.a_var = tf.Variable(1.0)
        self.b_var = tf.Variable(1.0)
        self.y_var = self.a_var * self.X + self.b_var
        self.loss = tf.reduce_mean(tf.square(self.y_var - self.Y))
        self.optimizer = tf.train.GradientDescentOptimizer(0.5)
        self.train = self.optimizer.minimize(self.loss)
        self.training_steps = 300
        self._results = []

    @property
    def results(self):
        return self._results

    def sample_training(self):
        print(self.y_var)
        with tf.Session() as sess:
            sess.run(tf.global_variables_initializer())
            for step in range(self.training_steps):
                self._results.append(sess.run([self.train, self.a_var, self.b_var])[1:])


class GetResult:

    def print_sample_result(self, results):
        final_pred = results[-1]
        a_hat = final_pred[0]
        b_hat = final_pred[1]
        print("a:", a_hat, "b:", b_hat)


if __name__ == '__main__':
    args = get_parser()
    data_gen = DataGeneration(args.a_var, args.b_var)
    data_gen.data_gen_sample_input()
    input_data, output_data = DataPrepare().sample_data_prepare(data_gen.input, data_gen.output)
    print(input_data)
    print(output_data)
    data = TrainModel(input_data, output_data)
    data.sample_training()
    GetResult().print_sample_result(data.results)




