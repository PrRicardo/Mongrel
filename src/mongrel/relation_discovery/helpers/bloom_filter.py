import mmh3

import numpy
import math


class BloomFilter:
    lookup: numpy.ndarray
    hash_functions: int
    size: int

    def __init__(self, expected_values: int, false_positive_acceptance: float = 0.00001):
        self.size = math.ceil(
            (expected_values * math.log(false_positive_acceptance)) / math.log(1 / pow(2, math.log(2))))
        self.lookup = numpy.zeros(self.size, dtype=bool)
        self.hash_functions = round((self.size / expected_values) * math.log(2))

    def hash(self, item: object):
        hash_value_life = mmh3.hash(str(item), 42, signed=False)
        hash_value_blaze = mmh3.hash(str(item), 420, signed=False)
        for i in range(self.hash_functions):
            yield (hash_value_life + i * hash_value_blaze) % self.size

    def check(self, item: object) -> bool:
        for i in self.hash(item):
            if not self.lookup[i]:
                return False
        return True

    def add(self, item: object, checked: bool = False):
        if not checked:
            if self.check(item):
                return
        for i in self.hash(item):
            self.lookup[i] = True

    def __contains__(self, item):
        return self.check(item)
