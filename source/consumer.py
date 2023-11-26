#TRY CATCH BLOCK, BOUNDS CHANGE, REGEX PATTERN 
import re
import time
import copy
import os
import math
import pandas as pd
import threading 
consumer_done = threading.Event()
consumer_temp = threading.Event()
consumer_fin = threading.Event()
# from generator import onesecond, tensecond
def find_pattern_in_windowBatch(window_start_id : int , window_end_id : int) -> int:
    regex = "AB"
    bounds = [['A', 2], ['B', 1]]
    start_time = time.time()
    #release results for every 10 second data
    df = pd.DataFrame(columns = ['timestamp', 'integer', 'char'])
    count = 0
    i = window_start_id
    while(i <= window_end_id):
        if os.path.exists('../data/unitTest.csv'):
            df1 = pd.read_csv('../data/unitTest.csv')
            df = pd.concat([df, df1], axis = 0)
        if(i%10 == 0 or i == window_end_id):  
            df = df.reset_index()
            rawData = ''.join(df['char'])
            pairs = findPairs(rawData, bounds, df)
            if(len(pairs.keys()) == len(regex)):
                length = len(pairs)
                result = findCount(pairs[length-1], pairs[length-2], length-2, pairs)
            else:
                result = 0
            count += result
            # print('The {}th window result is: {}'.format(math.ceil(i/10), result))
            df = pd.DataFrame()
        i+=1
    consumer_done.result = count
    consumer_done.set()
    # print("Consumer process has computed on the whole dataset in...{} seconds".format(time.time() - start_time))
    return count
def find_pattern_in_windowStream(thread_name : str , window_start_id : int , window_end_id : int) -> int:
    #Releases the count for a particular window
    regex = "AB"
    bounds = [['A', 2], ['B', 1]]
    df = pd.DataFrame()
    i = window_start_id
    while(i <= window_end_id):
        if os.path.exists('../data/unitTest.csv'):
            df1 = pd.read_csv('../data/unitTest.csv')
            df = pd.concat([df, df1])
        i+=1
    df = df.reset_index()
    rawData = ''.join(df['char'])
    result = 0
    pairs = findPairs(rawData, bounds, df)
    if(len(pairs.keys()) == len(regex)):
        length = len(pairs)
        result = findCount(pairs[length-1], pairs[length-2], length-2, pairs)
    else:
        result = 0
    # print('The result for this window is: {} and thread is {}'.format(result, thread_name))
    consumer_temp.result = result
    return [result, thread_name]
def findPairs(s : str, bounds : list, df: pd.core.frame.DataFrame) -> dict:
    m = len(s)
    # print("Size of the data from dataset: "+str(m))
    pairs = {}
    for i in range(len(bounds)):
        for j in range(len(s)):
            if(s[j] == bounds[i][0]):
                k = j+1
                while(k<len(s) and df.loc[k,'timestamp'] <= bounds[i][1]+df.loc[j, 'timestamp']):
                    if(s[k] == bounds[i][0]):
                        if i not in pairs:
                            pairs[i] = []
                            pairs[i].append((j,k))
                        else:
                            pairs[i].append((j, k))
                    k+=1
    return pairs
def findCount(curr : list, prev : list, k : int, pairs : dict) -> int:
    if(prev == []):
        return len(curr)
    temp = copy.deepcopy(prev)
    ans = 0
    for i in curr:
        executed = 0
        for j in prev:
            executed+=1
            if(i[0]>=j[0] and i[1]<=j[1]):
                pass
            else:
                temp.remove(j)
        if(k-1 < 0):
            ans += findCount(temp, [], k-1, pairs)
        else:
            ans += findCount(temp, pairs(k-1), k-1, pairs)
        temp = copy.deepcopy(prev)
    return ans
def findAverageBatch(window_start_id : int, window_end_id : int) -> int:
    start_time = time.time()
    df = pd.DataFrame(columns = ['timestamp', 'integer', 'value'])
    count = 0
    i = window_start_id
    while(i <= window_end_id):
        if os.path.exists('../data/unitTest.csv'):
            df1 = pd.read_csv('../data/unitTest.csv')
            df = pd.concat([df, df1], axis = 0)
        if(i%10 == 0 or i == window_end_id):  
            df = df.reset_index()
            count = avgg(df)
            # print('The {}th window result is: {}'.format(math.ceil(i/10), count))
            df = pd.DataFrame()
        i+=1
    consumer_done.result = count
    consumer_done.set()
    # print("Consumer process has computed on the whole dataset in...{} seconds".format(time.time() - start_time))
    return count
def findAverageStream(thread_name : str, window_start_id : int, window_end_id : int) -> int:
    df = pd.DataFrame(columns = ['timestamp', 'integer', 'value'])
    i = window_start_id
    while(i <= window_end_id):
        if os.path.exists('../data/unitTest.csv'):
            df1 = pd.read_csv('../data/unitTest.csv')
            df = pd.concat([df, df1])
        i+=1
    df = df.reset_index()
    result = avgg(df)
    # print('The result for this window is: {} and thread is {}'.format(result, thread_name))
    consumer_temp.result = result
    return [result, thread_name]
def avgg(df: pd.core.frame.DataFrame) -> int:
    ans = 0
    for i in range(len(df)):
        ans += df.loc[i, 'integer']
    return ans/len(df)
def findSumBatch(window_start_id : int, window_end_id : int) -> int:
    # print(window_start_id, window_end_id)
    start_time = time.time()
    #release results for every 10 second data
    df = pd.DataFrame(columns = ['timestamp', 'integer', 'char'])
    count = 0
    i = window_start_id
    while(i <= window_end_id):
        if os.path.exists('../data/unitTest.csv'):
            df1 = pd.read_csv('../data/unitTest.csv')
            df = pd.concat([df, df1], axis = 0)
        if(i%10 == 0 or i == window_end_id):  
            df = df.reset_index()
            count = summ(df)
            # print('The {}th window result is: {}'.format(math.ceil(i/10), ans))
            df = pd.DataFrame()
        i+=1
    consumer_done.result = count
    consumer_done.set()
    # print("Consumer process has computed on the whole dataset in...{} seconds".format(time.time() - start_time))
    return count
def findSumStream(thread_name : str, window_start_id : int, window_end_id : int) -> int:
    #Releases the count for a particular window
    df = pd.DataFrame(columns = ['timestamp', 'integer', 'char'])
    i = window_start_id
    while(i <= window_end_id):
        if os.path.exists('../data/unitTest.csv'):
            df1 = pd.read_csv('../data/unitTest.csv')
            df = pd.concat([df, df1])
        i+=1
    df = df.reset_index()
    result = summ(df)
    # print('The result for this window is: {} and thread is {}'.format(result, thread_name))
    consumer_temp.result = result
    return [result, thread_name]
def summ(df: pd.core.frame.DataFrame) -> int:
    ans = 0
    for i in range(len(df)):
        ans += df.loc[i, 'integer']
    return ans
def findMax(window_start_id : int, window_end_id : int) -> int:
    # print(window_start_id, window_end_id)
    start_time = time.time()
    #release results for every 10 second data
    df = pd.DataFrame(columns = ['timestamp', 'integer', 'value'])
    count = 0
    i = window_start_id
    while(i <= window_end_id):
        if os.path.exists('../data/unitTest.csv'):
            df1 = pd.read_csv('../data/unitTest.csv')
            df = pd.concat([df, df1], axis = 0)
        if(i%10 == 0 or i > window_end_id):  
            df = df.reset_index()
            count = maxx(df)
            # print('The {}th window result is: {}'.format(math.ceil(i/10), maxi))
            df = pd.DataFrame()
        i+=1
    consumer_done.result = count
    consumer_done.set()
    # print("Consumer process has computed on the whole dataset in...{} seconds".format(time.time() - start_time))
    return count
def findMaxStream(thread_name : str, window_start_id : int, window_end_id : int) -> int:
    #Releases the count for a particular window
    df = pd.DataFrame(columns = ['timestamp', 'integer', 'value'])
    i = window_start_id
    while(i <= window_end_id):
        if os.path.exists('../data/unitTest.csv'):
            df1 = pd.read_csv('../data/unitTest.csv')
            df = pd.concat([df, df1])
        i+=1
    df = df.reset_index()
    result = maxx(df)
    # print('The result for this window is: {} and thread is {}'.format(result, thread_name))
    consumer_temp.result = result
    return [result, thread_name]
def maxx(df: pd.core.frame.DataFrame) -> int:
    ans = -1
    for i in range(len(df)):
        ans = max(ans, df.loc[i, 'integer'])
    return ans
def findMini(window_start_id : int, window_end_id : int) -> int:
    # print(window_start_id, window_end_id)
    start_time = time.time()
    #release results for every 10 second data
    df = pd.DataFrame(columns = ['timestamp', 'integer', 'value'])
    count = 0
    i = window_start_id
    while(i <= window_end_id):
        if os.path.exists('../data/unitTest.csv'):
            df1 = pd.read_csv('../data/unitTest.csv')
            df = pd.concat([df, df1], axis = 0)
        if(i%10 == 0 or i == window_end_id):  
            df = df.reset_index()
            count = minn(df)
            # print('The {}th window result is: {}'.format(math.ceil(i/10), mini))
            df = pd.DataFrame()
        i+=1
    consumer_done.result = count
    consumer_done.set()
    # print("Consumer process has computed on the whole dataset in...{} seconds".format(time.time() - start_time))
    return count
def findMiniStream(thread_name : str, window_start_id : int, window_end_id : int) -> int:
    #Releases the count for a particular window
    df = pd.DataFrame(columns = ['timestamp', 'integer', 'value'])
    i = window_start_id
    while(i <= window_end_id):
        if os.path.exists('../data/unitTest.csv'):
            df1 = pd.read_csv('../data/unitTest.csv')
            df = pd.concat([df, df1])
        i+=1
    df = df.reset_index()
    result = minn(df)
    # print('The result for this window is: {} and thread is {}'.format(result, thread_name))
    consumer_temp.result = result
    return [result, thread_name]
def minn(df: pd.core.frame.DataFrame) -> int:
    ans = df.loc[0, 'integer']
    for i in range(len(df)):
        ans = min(ans, df.loc[i, 'integer'])
    return ans
def findVar(window_start_id : int, window_end_id : int) -> int:
    # print(window_start_id, window_end_id)
    start_time = time.time()
    #release results for every 10 second data
    df = pd.DataFrame(columns = ['timestamp', 'integer', 'value'])
    count = 0
    i = window_start_id
    while(i <= window_end_id):
        if os.path.exists('../data/unitTest.csv'):
            df1 = pd.read_csv('../data/unitTest.csv')
            df = pd.concat([df, df1], axis = 0)
        if(i%10 == 0 or i == window_end_id):  
            df = df.reset_index()
            count = calculate_variance(df)
            # print('The {}th window result is: {}'.format(math.ceil(i/10), variance))
            df = pd.DataFrame()
        i+=1
    consumer_done.result = count
    consumer_done.set()
    # print("Consumer process has computed on the whole dataset in...{} seconds".format(time.time() - start_time))
    return count
def findVarStream(thread_name : str, window_start_id : int, window_end_id : int) -> int:
    #Releases the count for a particular window
    df = pd.DataFrame(columns = ['timestamp', 'integer', 'value'])
    i = window_start_id
    while(i <= window_end_id):
        if os.path.exists('../data/unitTest.csv'):
            df1 = pd.read_csv('../data/unitTest.csv')
            df = pd.concat([df, df1])
        i+=1
    df = df.reset_index()
    result = calculate_variance(df)
    # print('The result for this window is: {} and thread is {}'.format(result, thread_name))
    consumer_temp.result = result
    return [result, thread_name]
def calculate_variance(dataframe: pd.core.frame.DataFrame) -> int:
    # print('ok')
    column_name = "integer"
    # Extract the specified column as a list
    values = dataframe[column_name].tolist()
    # Calculate the mean
    mean = sum(values) / len(values)
    # Calculate the sum of squared differences from the mean
    squared_diff = sum((x - mean) ** 2 for x in values)
    # Calculate the variance
    variance = squared_diff / len(values)
    return variance
def findStd(window_start_id : int, window_end_id : int) -> int:
    # print(window_start_id, window_end_id)
    start_time = time.time()
    #release results for every 10 second data
    df = pd.DataFrame(columns = ['timestamp', 'integer', 'value'])
    count = 0
    i = window_start_id
    while(i <= window_end_id):
        if os.path.exists('../data/unitTest.csv'):
            df1 = pd.read_csv('../data/unitTest.csv')
            df = pd.concat([df, df1], axis = 0)
        if(i%10 == 0 or i == window_end_id):  
            df = df.reset_index()
            count = calculate_std_dev(df)
            # print('The {}th window result is: {}'.format(math.ceil(i/10), standardDeviation))
            df = pd.DataFrame()
        i+=1
    consumer_done.result = count
    consumer_done.set()
    # print("STD IS RETURNING - ", count)
    # print("Consumer process has computed on the whole dataset in...{} seconds".format(time.time() - start_time))
    return count
def findStdStream(thread_name : str, window_start_id : int, window_end_id : int) -> int:
        df = pd.DataFrame(columns = ['timestamp', 'integer', 'value'])
        i = window_start_id
        while(i <= window_end_id):
            if os.path.exists('../data/unitTest.csv'):
                df1 = pd.read_csv('../data/unitTest.csv')
                df = pd.concat([df, df1])
            i+=1
        df = df.reset_index()
        result = calculate_std_dev(df)
        # print('The result for this window is: {} and thread is {}'.format(result, thread_name))
        consumer_temp.result = result
        # print("STD IS RETURNING -", result)
        return [result, thread_name]
def calculate_std_dev(dataframe: pd.core.frame.DataFrame) -> int:
    column_name = "integer"
    values = dataframe[column_name].tolist()
    mean = sum(values) / len(values)
    squared_diff = sum((x - mean) ** 2 for x in values)
    variance = squared_diff / len(values)
    std_dev = math.sqrt(variance)
    return std_dev
# def custom_groupby(dataframe: pd.core.frame.DataFrame, groupby_column: str) -> dict:
#     grouped_data = {}
#     for index, row in dataframe.iterrows():
#         key = row[groupby_column]
#         if key not in grouped_data:
#             grouped_data[key] = {'Values': [row['value']], 'Count': 1, 'Max': row['value'], 'Min': row['value'], 'Sum': row['value']}
#         else:
#             grouped_data[key]['Values'].append(row['Value'])
#             grouped_data[key]['Count'] += 1
#             grouped_data[key]['Max'] = max(grouped_data[key]['Max'], row['value'])
#             grouped_data[key]['Min'] = min(grouped_data[key]['Max'], row['value'])
#             grouped_data[key]['Sum'] += row['value']
#     return grouped_data
# find_pattern_in_windowBatch(regex, bounds, 1, 10)
# if __name__ == "__main__":
#     file_name = 'stream_data.csv'  # Use the same filename generated by the generator program
#     # regex_pattern = "[BCDFGHJKLMNPQRSTVWXYZ][AEIOU]+[BCDFGHJKLMNPQRSTVWXYZ]?"
#     regex = "AB"
#     bounds = [['A', 3], ['B', 2]]
#     window_duration = 2
#     duration = 2
#     df = pd.DataFrame(columns = ['timestamp', 'integer', 'value'])
#     if os.path.exists('../data/unitTest.csv'):
#         df1 = pd.read_csv('../data/unitTest.csv')
#         df = pd.concat([df, df1], axis = 0)
#     print(findPairs("BTAAINABBA", [['A', 2], ['B', 1]],df=df))
    # print(find_pattern_in_windowBatch(1, 1))
    # print(findStd(1, 1))