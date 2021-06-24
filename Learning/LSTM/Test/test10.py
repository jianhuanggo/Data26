import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

import json

import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import pandas_datareader as web
import datetime as dt
from sklearn.preprocessing import MinMaxScaler
import tensorflow as tf
from tensorflow.keras.layers import Dense, Dropout, LSTM
from tensorflow.keras.models import Sequential, load_model, save_model
from typing import List
from typing import Generic, TypeVar, Optional, List
from pydantic import BaseModel, validator, ValidationError
from pydantic.generics import GenericModel
from Data.Utils import pgfile


import pandas as pd

DataFrame = TypeVar('DataFrame')


class Data(GenericModel, Generic[DataFrame]):
    symbol: str
    data_frame: Optional[DataFrame]
    size: int = None
    """
    class Config:
        json_encoders = {
            Optional[DataFrame]: lambda x: x.to_json(),
        }
    """


def save():
    data = web.DataReader(f"BTC-USD", "yahoo", dt.datetime(2016, 1, 1), dt.datetime.now())
    external_data = {
        'symbol': "BTC-USD",
        #'data_frame': data.to_json(orient='records', index=True),
        'data_frame': data,
        'size': 10,
    }
    external_data['data_frame']['Date'] = external_data['data_frame'].index
    external_data['data_frame'] = external_data['data_frame'].to_json(orient='records')
    print(external_data['data_frame'])

    with open('/Users/jianhuang/opt/anaconda3/envs/Data20/Data20/Learning/LSTM/Test/saved_symbol1.txt', 'w') as file:
        file.write(Data(**external_data).json())


def train(x_train, y_train, strategy):
    model_strategy_map = {'mirroredstrategy': tf.distribute.MirroredStrategy(),
                          'centralStoragestrategy': tf.distribute.experimental.CentralStorageStrategy()
    }
    distribution = model_strategy_map.get(strategy)

    with distribution.scope():
        model = Sequential()
        model.add(LSTM(units=50, return_sequences=True, input_shape=(x_train.shape[1], 1)))
        model.add(Dropout(0.2))
        model.add(LSTM(units=50, return_sequences=True))
        model.add(Dropout(0.2))
        model.add(LSTM(units=50))
        model.add(Dropout(0.2))
        model.add(Dense(units=1))

        model.compile(optimizer='adam', loss="mean_squared_error")
        model.fit(x_train, y_train, epochs=10, batch_size=32)
        model.save(f"/Users/jianhuang/opt/anaconda3/envs/Data20/Data20/Learning/LSTM/Test/btc-usd-train_60days-predict_nextday.{strategy}.h5")
    return model


def load_model(filepath, strategy):
    model_strategy_map = {'mirroredstrategy': tf.distribute.MirroredStrategy(),
                          'centralStoragestrategy': tf.distribute.experimental.CentralStorageStrategy()
    }

    distribution = model_strategy_map.get(strategy)
    with distribution.scope:
        return load_model(filepath)

def load():
    with open('/Users/jianhuang/opt/anaconda3/envs/Data20/Data20/Learning/LSTM/Test/saved_symbol1.txt', 'r') as file:
        _data = pd.read_json(Data(**json.loads(file.readlines()[0])).data_frame, orient='records')
    _data.set_index('Date', inplace=True)
    return _data


if __name__ == '__main__':

    _data = load()
    _scaler = MinMaxScaler(feature_range=(0, 1))

    # (-1, 1) setting to dimension/column to 1
    _scaled_data = _scaler.fit_transform(_data['Close'].values.reshape(-1, 1))

    print(f"scaled data shape: {_scaled_data.shape}")

    prediction_days = 60

    x_train, y_train = [], []

    for x in range(prediction_days, len(_scaled_data)):
        x_train.append(_scaled_data[x-prediction_days: x, 0])
        y_train.append(_scaled_data[x, 0])

    x_train, y_train = np.array(x_train), np.array(y_train)
    x_train = np.reshape(x_train, (x_train.shape[0], x_train.shape[1], 1))

    # Create Neutral Network
    if pgfile.isfileexist('/Users/jianhuang/opt/anaconda3/envs/Data20/Data20/Learning/LSTM/Test/btc-usd-train_60days-predict_nextday.h5'):
        model = load_model('/Users/jianhuang/opt/anaconda3/envs/Data20/Data20/Learning/LSTM/Test/btc-usd-train_60days-predict_nextday.h5')
    else:
        model = train(x_train, y_train, "centralStoragestrategy")

    test_start = dt.datetime(2020, 1, 1)
    test_end = dt.datetime.now()

    test_data = web.DataReader(f"BTC-USD", 'yahoo', test_start, test_end)

    print(f"length of test data: {len(test_data)}")

    actual_prices = test_data['Close'].values
    total_dataset = pd.concat((_data['Close'], test_data['Close']), axis=0)
    model_inputs = total_dataset[len(total_dataset) - len(test_data) - prediction_days:].values
    model_inputs = model_inputs.reshape(-1, 1)
    model_inputs = _scaler.fit_transform(model_inputs)

    x_test = []
    x_test1 = []

    for x in range(prediction_days, len(model_inputs)):
        x_test.append(model_inputs[x-prediction_days: x, 0])

    x_test = np.array(x_test)
    x_test = np.reshape(x_test, (x_test.shape[0], x_test.shape[1], 1))

    prediction_prices = model.predict(x_test)
    prediction_prices = _scaler.inverse_transform(prediction_prices)

    #print(prediction_prices)

    plt.plot(actual_prices, color='black', label='Actual Prices')
    plt.plot(prediction_prices, color='green', label='Predicted Price')

    plt.title("BTC price prediction")
    plt.xlabel('Time')
    plt.ylabel('Price')
    plt.legend(loc='upper left')
    plt.show()






    
