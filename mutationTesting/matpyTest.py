from unittest import TestCase
import sys
sys.path.append('../')
from source.consumer import find_pattern_in_windowBatch, find_pattern_in_windowStream
from source.consumer import findCount, findMini, findPairs, findAverageBatch, findAverageStream, findMax, findMaxStream, findMiniStream, findStdStream, findStd, findVar, findVarStream, findSumBatch, findSumStream
from source.consumer import avgg, calculate_std_dev, calculate_variance, summ, maxx, minn
import pandas as pd
import os

df = pd.DataFrame(columns = ['timestamp', 'integer', 'value'])
if os.path.exists('../data/unitTest.csv'):
    df1 = pd.read_csv('../data/unitTest.csv')
    df = pd.concat([df, df1], axis = 0)

print("avg: "+str(avgg(df)))
print("sum: "+str(summ(df)))
print("max: "+str(maxx(df)))
print("min: "+str(minn(df)))
print("std: "+str(calculate_std_dev(df)))
print("var: "+str(calculate_variance(df)))

class CalculatorTest(TestCase):

    def test_generateData(self):
        self.assertEqual(findPairs("BTAAINABBA", [['A', 2], ['B', 1]],df=df), {0: [(2, 3), (2, 6), (2, 9), (3, 6), (3, 9), (6, 9)], 1: [(0, 7), (0, 8), (7, 8)]})
        self.assertEqual(findCount(curr = [(2, 5)], prev = [(0, 3), (0, 6), (3, 6), (6, 11), (6, 13), (11, 13)], k = 0, pairs = {0: [(0, 3), (0, 6), (3, 6), (6, 11), (6, 13), (11, 13)], 1: [(2, 5)]}), 1)
        self.assertEqual(avgg(df), 4.8)
        self.assertEqual(summ(df), 48)
        self.assertEqual(maxx(df), 9)
        self.assertEqual(minn(df), 1)
        self.assertEqual(find_pattern_in_windowBatch(1, 1), 3)
        self.assertEqual(find_pattern_in_windowStream("thread_1", 1, 1)[0], 3)
        self.assertEqual(findAverageBatch(1, 1), 4.8)
        self.assertEqual(findAverageStream("thread_1", 1, 1)[0], 4.8)
        self.assertEqual(findSumBatch(1, 1), 48)
        self.assertEqual(findSumStream("thread_1", 1, 1)[0], 48)
        # self.assertEqual(findMax(1, 1), 9)
        # self.assertEqual(findMaxStream("thread_1", 1, 1)[0], 9)
        self.assertEqual(findMini(1, 1), 1)
        self.assertEqual(findMiniStream("thread_1", 1, 1)[0], 1)
        self.assertEqual(findVar(1, 1), 10.36)
        self.assertEqual(findVarStream("thread_1", 1, 1)[0], 10.36)
        self.assertEqual(findStd(1, 1), 3.2186953878862163)
        self.assertEqual(findStdStream("thread_1", 1, 1)[0], 3.2186953878862163)
        self.assertEqual(calculate_std_dev(df), 3.2186953878862163)
        self.assertEqual(calculate_variance(df), 10.36)