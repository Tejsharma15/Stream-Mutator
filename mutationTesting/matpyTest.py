from unittest import TestCase
# from consumer import find_pattern_in_windowBatch, find_pattern_in_windowStream
from consumer import findCount, findMini
from consumer import avgg, calculate_std_dev, calculate_variance, summ, maxx, minn
import pandas as pd
import os

df = pd.DataFrame(columns = ['timestamp', 'integer', 'value'])
if os.path.exists('./data/unitTest.csv'):
    df1 = pd.read_csv('./data/unitTest.csv')
    df = pd.concat([df, df1], axis = 0)

print("avg: "+str(avgg(df)))
print("sum: "+str(summ(df)))
print("max: "+str(maxx(df)))
print("min: "+str(minn(df)))
print("std: "+str(calculate_std_dev(df)))
print("var: "+str(calculate_variance(df)))

class CalculatorTest(TestCase):

    def test_generateData(self):
        self.assertEqual(findCount(curr = [(2, 5)], prev = [(0, 3), (0, 6), (3, 6), (6, 11), (6, 13), (11, 13)], k = 0, pairs = {0: [(0, 3), (0, 6), (3, 6), (6, 11), (6, 13), (11, 13)], 1: [(2, 5)]}), 1)
        self.assertEqual(avgg(df), 5.35)
        self.assertEqual(summ(df), 107)
        self.assertEqual(maxx(df), 10)
        self.assertEqual(minn(df), 1)
        self.assertEqual(findMini(1, 2), 1)
        self.assertEqual(calculate_std_dev(df), 3.1823733281939126)
        self.assertEqual(calculate_variance(df), 10.1275)