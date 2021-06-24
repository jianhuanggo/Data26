import numpy as np
import inspect
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from typing import Callable, Union, Any, TypeVar, Tuple, Iterable, Generator
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder, OneHotEncoder, MinMaxScaler
from sklearn.compose import ColumnTransformer
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score
from Meta import pggenericfunc



_NUM_SAMPLES = 5

def is_dummy_variable(a: np.array):
    b = [[1.1, 1.1],[1.1, 1.1]]
    print(np.multiply(b, np.transpose(b)))
    #print(np.multiply(c, np.transpose(c)))
    c = np.dot(a, np.transpose(a))
    print(c)
    print(type(c[0][0]))
    print(type(b[0][0]))
    print(c.dtype)
    d = c.astype('float64')
    ainv = np.linalg.inv(d)
    print(ainv)
    print(np.linalg.det(d))


# np.ndarray
def pg_convert_categorical_data(data: pd.DataFrame, columns: Union[list, str] = "auto", _logger=None):
    def _dict_update(dict, index, val):
        dict[index] = val

    labelencoder = LabelEncoder()
    _data = data.to_numpy()
    try:
        if columns == "auto":
            _data_type = {_index: "0" for (_index, _) in enumerate(data.columns)}
            _samples = ((_index, _d) for _index, _d in enumerate(data.sample().to_numpy()[0]) for _ in range(_NUM_SAMPLES))
            list(map(lambda x: _dict_update(_data_type, x[0], type(x[1])) if not isinstance(x[1], (float, int, complex)) else "0", _samples))
            columns = [_key for _key, _val in _data_type.items() if _val != "0"]
            print(columns)

        for item in columns:
            _data[:, item] = labelencoder.fit_transform(_data[:, item])
            ct = ColumnTransformer([(data.columns[item], OneHotEncoder(drop='first'), [item])], remainder='passthrough')
            _data = ct.fit_transform(_data)

        return _data
    except Exception as err:
        pggenericfunc.pg_error_logger(_logger, inspect.currentframe().f_code.co_name, err)
        return None



companies = pd.read_csv('test10.csv')
X = companies.iloc[:, :-1].values
y = companies.iloc[:, 4].values
print(companies.head())
#ax = sns.heatmap(companies.corr())
#plt.show()

labelencoder = LabelEncoder()
X[:, 3] = labelencoder.fit_transform(X[:, 3])
print(X)
ct = ColumnTransformer([("State", OneHotEncoder(), [3])], remainder='passthrough')
X = ct.fit_transform(X)
print(X)
print(X.dtype)


#print(pg_convert_categorical_data(companies, [3]))
_processed_data = pg_convert_categorical_data(companies, "auto")
print(_processed_data)
_pt = MinMaxScaler(feature_range=(0, 1))
_processed_data = _pt.fit_transform(_processed_data)
print(_processed_data)

_X = _processed_data[:, :-1]
_y = _processed_data[:, -1]

print(_X)
print(X)
print()
print()
print(_y)
print(y)


X_train, X_test, y_train, y_test = train_test_split(_X, _y, test_size=0.2, random_state=0)
regressor = LinearRegression()
regressor.fit(X_train, y_train)

y_pred = regressor.predict(X_test)
print(y_pred)

print(r2_score(y_test, y_pred))

#is_dummy_variable(X)

exit(0)
#print(X)
#X = X[:, 1:]



#print(X)


