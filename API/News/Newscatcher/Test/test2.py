from typing import Iterable


a = [[0,1,0], [1,0,0], [1,1,0], [0,1,0], [0,1,1]]

#     [1,0,0], [1,1,0], [0,1,0], [0,1,1]

#              [0,1,0], [1,0,0], [1,1,0], [0,1,0]

#     -----------------------------------
#     [1,1,0], [1,1,0], [1,1,0], [1,1,1], [0,1,1]


class FVBlock:

    def __init__(self):
        self._a = [[1,0,0], [0,0,0], [0,1,0], [0,1,0], [0,1,1]]
        self._distance = 0

    @staticmethod
    def check(block_requirement: list) -> bool:
        for item in block_requirement:
            if not item:
                return False
        return True

    @staticmethod
    def list_op(list1: list, list2: list, list3: list) -> list:
        print(f"list1: {list1} and list2: {list2}")
        return list(map(lambda x: x[0] | x[1] | x[2], list(zip(list1, list2, list3))))

    def main(self):
        b = self._a
        _flag = False
        while True:
            self._a = b.copy()
            for index, item in enumerate(self._a):
                if self.check(item):
                    print(f"the block {index} is the best choice")
                    _flag = True
                    break
                if index > 0:
                    b[index] = self.list_op(b[index], self._a[index], self._a[index-1])
                if index < len(self._a) - 1:
                    b[index] = self.list_op(b[index], self._a[index], self._a[index+1])
            self._distance += 1
            if _flag == True:
                break
        print(self._distance)


class FindBadEngineer:
    def __init__(self):
        self._build_series = [[True, True, True, False, False],
                              [True, True, True, True, False],
                              [True, True, True, True, True, True, False, False, False],
                              [True, False, False, False, False, False],
                              [True, True,True, True,True, True, True,True, True,True, True, True, False],
                              [True, False],
                              [True, True,True, True, False, False]
                            ]
        self._consecutive_decline = 0
        self._last_pointer = None
        self._last_pointer = None
        self._current_pointer = None
        self._great_decline_day = 0

    def cal_build_percent(self, iter: Iterable) -> int:
        _num_good_build = 0
        for build in iter:
            _num_good_build += 1 if build else 0
        return _num_good_build / len(iter)

    def parse_entire_build(self, iter: Iterable) -> int:
        _build_score = []
        for _build_day in iter:
            _build_score.append(self.cal_build_percent(_build_day))

        if _build_score:
            self._last_pointer = _build_score.pop()
            print(self._last_pointer)
        _consecutive_decline = 0
        while _build_score:
            self._current_pointer = _build_score.pop()
            if self._current_pointer <= self._last_pointer:
                _consecutive_decline += 1
            elif self._current_pointer > self._last_pointer:
                self._great_decline_day = max(self._great_decline_day, _consecutive_decline)
                _consecutive_decline = 0
            print(self._last_pointer, self._current_pointer, self._great_decline_day, _consecutive_decline)
            self._last_pointer = self._current_pointer

        print(self._great_decline_day)


if __name__ == '__main__':
    FVBlock().main()
    test = FindBadEngineer()
    test.parse_entire_build(test._build_series)





