# Wentao Zhou
# DSCI 553 - data mining, Spring 2021, HW3, task2_1
# recommendation system - item-based CF

from pyspark import SparkContext
import sys
import random
import itertools
import math
from sklearn.metrics import mean_squared_error as MSE
import numpy as np
import time

start = time.time()
sc = SparkContext('local[*]', 'task2_1')
sc.setLogLevel("ERROR")

# compute the similarity of two items
# basically follow the steps from slides
def computeSimilarity(co_rated_user, dic, business_tobepredict, business, averageBusi_rdd):
    if len(co_rated_user) <= 2: # has very few co-rated items
        return 5 / (float(abs(averageBusi_rdd[business_tobepredict] - averageBusi_rdd[business]))+1)
    r1 = 0
    r2 = 0
    for user in co_rated_user:
        r1 += dic[business_tobepredict][user]
        r2 += dic[business][user]
    r1 = r1/len(co_rated_user)
    r2 = r2/len(co_rated_user)
    numerator = 0
    for user in co_rated_user:
        a = dic[business_tobepredict][user]-r1
        b = dic[business][user]-r2
        numerator += a*b
    if numerator == 0:
        return 0
    denominator1 = 0
    denominator2 = 0
    for user in co_rated_user:
        denominator1 += (dic[business_tobepredict][user]-r1) ** 2
        denominator2 += (dic[business][user] - r2) ** 2
    denominator1 = math.sqrt(denominator1)
    denominator2 = math.sqrt(denominator2)
    return numerator / (denominator1*denominator2)

# return (similarity, (user, other business), (user, business))
def getSimilar(training, dic, user_busi_dic, averageBusi_rdd, averageUser_rdd):
    business_tobepredict = training[0]
    user_tobepredict = training[1]
    res = []
    if business_tobepredict not in dic.keys(): # if business is not in training dataset, I guess the similarity is 4.
        res.append((4,('no_busi', 'no_user'), (user_tobepredict, business_tobepredict)))
        return res
    if user_tobepredict not in user_busi_dic.keys(): # if user is not in training dataset, I guess the similarity is 4.
        res.append((4, ('no_busi', 'no_user'), (user_tobepredict, business_tobepredict)))
        return res
    setofuser_tobepredict = set(dic[business_tobepredict].keys())
    for business in user_busi_dic[user_tobepredict]:
        listofuser_other = set(dic[business].keys())
        co_rated_user = setofuser_tobepredict.intersection(listofuser_other)
        #if len(co_rated_user) > 0:
        #similarity = 0
        similarity = computeSimilarity(co_rated_user, dic, business_tobepredict, business, averageBusi_rdd)
        if similarity > 0: # get rid of negative similarity
            res.append((similarity, (user_tobepredict, business), (user_tobepredict, business_tobepredict)))
    if len(res) == 0:
        res.append((4,('no_busi', 'isuser'), (user_tobepredict, business_tobepredict)))
    res = sorted(res, key = lambda x: x[0], reverse=True)
    return res

# calculate predictions, follow the steps from slides
def getprediction(x, dic):
    numerator = 0
    denominator = 0
    for row in x:
        if row[1][0] == 'no_busi':
            return [x[0][2][0], x[0][2][1], row[0]]
        if row[1][0] in dic[row[1][1]].keys():
            numerator += dic[row[1][1]][row[1][0]]*row[0]
            denominator += row[0]
    if numerator == 0 or denominator == 0:
        return [x[0][2][0], x[0][2][1], 4]
    res = numerator / denominator
    return [x[0][2][0], x[0][2][1], res]

# don't need the items beyond 10th
def filterout(x,n):
    lis = []
    if n > len(x):
        n = len(x)
    for i in range(0,n):
        lis.append(x[i])
    return lis

#input_file_path = "train_slides.csv"
#train_data = "self_val_in.csv"

input_file_path = sys.argv[1]
train_data = sys.argv[2]
output_file_path = sys.argv[3]
first_line = sc.textFile(input_file_path).first()
rdd = sc.textFile(input_file_path).filter(lambda row: row != first_line)
dic_rdd = rdd.map(lambda row: (str(row.split(',')[1]), (str(row.split(',')[0]), float(row.split(',')[2])))).groupByKey().mapValues(dict)
averageBusi_rdd = rdd.map(lambda row: (str(row.split(',')[1]), float(row.split(',')[2]))).groupByKey().mapValues(list).map(lambda x: (x[0], sum(x[1])/len(x[1])))
averageUser_rdd = rdd.map(lambda row: (str(row.split(',')[0]), float(row.split(',')[2]))).groupByKey().mapValues(list).map(lambda x: (x[0], sum(x[1])/len(x[1])))

averageBusi_dic = dict(averageBusi_rdd.collect())
averageUser_dic = dict(averageUser_rdd.collect())

user_busi_rdd = rdd.map(lambda row: (str(row.split(',')[0]), str((row.split(',')[1])))).groupByKey().mapValues(list)
user_busi_dic = dict(user_busi_rdd.collect())


dic = dict(dic_rdd.collect())

N = 10 # number of neighborhood item
first_line_train = sc.textFile(train_data).first()
training = sc.textFile(train_data).filter(lambda row: row != first_line)\
    .map(lambda row: (str(row.split(',')[1]), str(row.split(',')[0]))).map(lambda x: getSimilar(x, dic, user_busi_dic, averageBusi_dic, averageUser_dic))\
    .map(lambda x: filterout(x, N)) # the training data

predict = training.map(lambda x: getprediction(x, dic)).collect()

predict = np.array(predict)

with open(output_file_path, 'w') as output:
    output.write("user_id, business_id, prediction\n")
    for pair in predict:
        output.write(str(pair[0]) + "," + str(pair[1]) + "," + str(pair[2]) + "\n")
print("Duration: " + str(time.time()-start))