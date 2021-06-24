import numpy as np
import pandas as pd


def pgdp(capacity: float, items: pd.DataFrame):
    """
    [item, cost, value]
    .values
    """
    _cost = items.iloc[:, 1]
    _value = items.iloc[:, 2]
    _least_granuity = min(_cost)

    K = [[0 for x in np.arange(0, capacity + _least_granuity, _least_granuity)] for x in range(len(items) + 1)]
    print(K)
    print(_cost.values)
    print(_value.values)
    print((capacity + _least_granuity) / _least_granuity)

    """
    for i in range(1, len(items)):
        for w in range(1, int((capacity)/_least_granuity)):
            K[i][w] = 1
    """
    print(K)

    print([i for i in range(len(items) + 1)])

    for i in range(len(items) + 1):
        for w in range(int((capacity) / _least_granuity) + 1):
            if i == 0 or w == 0:
                K[i][w] = 0
            elif _cost[i - 1] <= _least_granuity * w:
                K[i][w] = max(_value[i - 1] + K[i - 1][int((w * _least_granuity - _cost[i - 1])/_least_granuity)], K[i - 1][w])
            else:
                K[i][w] = K[i - 1][w]

        print(K)


def pgdpwithassignment(capacity: float, items: pd.DataFrame):
    """
    [item, cost, value]
    .values
    """
    _item = items.iloc[:, 0]
    _cost = items.iloc[:, 1]
    _value = items.iloc[:, 2]
    _least_granuity = min(_cost)

    K = [[0 for x in np.arange(0, capacity + _least_granuity, _least_granuity)] for x in range(len(items) + 1)]
    A = [["" for x in np.arange(0, capacity + _least_granuity, _least_granuity)] for x in range(len(items) + 1)]

    for i in range(len(items) + 1):
        for w in range(int((capacity) / _least_granuity) + 1):
            if i == 0 or w == 0:
                K[i][w] = 0
            elif _cost[i - 1] <= _least_granuity * w:
                _part_one = _value[i - 1] + K[i - 1][int((w * _least_granuity - _cost[i - 1])/_least_granuity)]
                _part_two = K[i - 1][w]
                if _part_one > _part_two:
                    K[i][w] = _part_one
                    A[i][w] = f"{_item[i - 1]}, {A[i - 1][int((w * _least_granuity - _cost[i - 1])/_least_granuity)]}"
                else:
                    K[i][w] = _part_two
                    A[i][w] = A[i - 1][w]

            else:
                K[i][w] = K[i - 1][w]
                A[i][w] = A[i - 1][w]

    print(K)
    print(A)


def pgdp_scaler(capacity, val: [pd.DataFrame, list]):
    _scale_factor = 1000
    _min = min(val)
    while _min * _scale_factor < 1000:
        _scale_factor *= 10
    return int(capacity * _scale_factor), [int(x * _scale_factor) for x in val]


def pgdp2_withassignment(capacity: int, items: pd.DataFrame):

    _capacity, _cost = pgdp_scaler(capacity, items.iloc[:, 1])
    _value = items.iloc[:, 2]

    _pg = [[0 for x in range(_capacity + 1)] for x in range(len(items) + 1)]
    _explain = [[[] for x in range(_capacity + 1)] for x in range(len(items) + 1)]

    for i in range(len(items) + 1):
        for w in range(_capacity + 1):
            if i == 0 or w == 0:
                _pg[i][w] = 0
            elif _cost[i-1] <= w:
                _current_max = _value[i - 1] + _pg[i - 1][w - _cost[i - 1]]
                _previous_max = _pg[i - 1][w]
                if _current_max > _previous_max:
                    _pg[i][w] = _current_max
                    _explain[i][w] = [i - 1] + _explain[i - 1][w - _cost[i - 1]]
                else:
                    _pg[i][w] = _previous_max
                    _explain[i][w] = _explain[i - 1][w]

            else:
                _pg[i][w] = _pg[i - 1][w]
                _explain[i][w] = _explain[i - 1][w]

    return _pg[len(items)][_capacity], _explain[len(items)][_capacity]


def pgdp2(capacity: float, items: pd.DataFrame):

    _capacity, _cost = pgdp_scaler(capacity, items.iloc[:, 1])
    _value = items.iloc[:, 2]
    pg = [[0 for x in range(_capacity + 1)] for x in range(len(items) + 1)]

    for i in range(len(items) + 1):
        for w in range(_capacity + 1):
            if i == 0 or w == 0:
                pg[i][w] = 0
            elif _cost[i - 1] <= w:
                pg[i][w] = max(_value[i - 1] + pg[i - 1][w - _cost[i - 1]],  pg[i - 1][w])
            else:
                pg[i][w] = pg[i - 1][w]
    return pg[len(items)][_capacity]


if __name__ == '__main__':
    #test = pd.read_csv('test.csv', header=0)
    #print(test)
    #pgdp(2, test)
    #pgdpwithassignment(2, test)
    #print(pgdp2(2, pd.read_csv('test.csv', header=0)))
    print(pgdp2_withassignment(4, pd.read_csv('test.csv', header=0)))
    print(pgdp2(2, pd.read_csv('test.csv', header=0)))

