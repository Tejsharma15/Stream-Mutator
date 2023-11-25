from unittest import TestCase
import importlib
import sys
sys.path.append('../')

class IntegrationTests(TestCase):
    def reload_test_module(self):
        importlib.invalidate_caches()
        importlib.reload(importlib.import_module("source.consumer"))

    def test_find_pattern_in_windowBatch(self):
        self.reload_test_module()
        from source.consumer import find_pattern_in_windowBatch
        self.assertEqual(find_pattern_in_windowBatch(1, 1), 3)

    def test_find_pattern_in_windowStream(self):
        self.reload_test_module()
        from source.consumer import find_pattern_in_windowStream
        self.assertEqual(find_pattern_in_windowStream("thread_1", 1, 1)[0], 3)

    def test_findAverageBatch(self):
        self.reload_test_module()
        from source.consumer import findAverageBatch
        self.assertEqual(findAverageBatch(1, 1), 4.8)

    def test_findAverageStream(self):
        self.reload_test_module()
        from source.consumer import findAverageStream
        self.assertEqual(findAverageStream("thread_1", 1, 1)[0], 4.8)

    def test_findSumBatch(self):
        self.reload_test_module()
        from source.consumer import findSumBatch
        self.assertEqual(findSumBatch(1, 1), 48)

    def test_findSumStream(self):
        self.reload_test_module()
        from source.consumer import findSumStream
        self.assertEqual(findSumStream("thread_1", 1, 1)[0], 48)

    def test_findMax(self):
        self.reload_test_module()
        from source.consumer import findMax
        self.assertEqual(findMax(1, 1), 9)

    def test_findMaxStream(self):
        self.reload_test_module()
        from source.consumer import findMaxStream
        self.assertEqual(findMaxStream("thread_1", 1, 1)[0], 9)

    def test_findMini(self):
        self.reload_test_module()
        from source.consumer import findMini
        self.assertEqual(findMini(1, 1), 1)

    def test_findMiniStream(self):
        self.reload_test_module()
        from source.consumer import findMiniStream
        self.assertEqual(findMiniStream("thread_1", 1, 1)[0], 1)

    def test_findVar(self):
        self.reload_test_module()
        from source.consumer import findVar
        self.assertEqual(findVar(1, 1), 10.36)

    def test_findVarStream(self):
        self.reload_test_module()
        from source.consumer import findVarStream
        self.assertEqual(findVarStream("thread_1", 1, 1)[0], 10.36)

    def test_findStd(self):
        self.reload_test_module()
        from source.consumer import findStd
        self.assertEqual(findStd(1, 1), 3.2186953878862163)

    def test_findStdStream(self):
        self.reload_test_module()
        from source.consumer import findStdStream
        self.assertEqual(findStdStream("thread_1", 1, 1)[0], 3.2186953878862163)

    # def test_func1(self):
    #     self.reload_test_module()
    #     from test import func2
    #     self.assertEqual(func1(2, "656"), 3) 


    # def test_func3(self):
    #     self.reload_test_module()
    #     from test import func3
    #     self.assertEqual(func3(), 6)   
