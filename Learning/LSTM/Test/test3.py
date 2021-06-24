import numpy as np
from tensorflow import keras

reconstructed_model = keras.models.load_model("/Users/jianhuang/opt/anaconda3/envs/Data20/Data20/Learning/LSTM/model/entity_name-BTC-USD_num_consecutive_inputs-60_position_offset_for_output-0_batch_size-32.h5")