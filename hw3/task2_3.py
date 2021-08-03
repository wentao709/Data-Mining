# Wentao Zhou
# DSCI 553 - data mining, Spring 2021, HW3, task2_3
# recommendation system: hybrid recommendation system.
# I combine task2_1 and task2_2 by weighted average

import math
import time
import xgboost as xgb
from sklearn.metrics import mean_squared_error as MSE
import numpy as np
import itertools
import sys
from pyspark import SparkContext
import json

start = time.time()
sc = SparkContext('local[*]', 'task2_3')
sc.setLogLevel("ERROR")

def computeSimilarity(co_rated_user, dic, business_tobepredict, business, averageBusi_rdd):
    if len(co_rated_user) <= 2:
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

def getSimilar(training, dic, user_busi_dic, averageBusi_rdd, averageUser_rdd):
    business_tobepredict = training[0]
    user_tobepredict = training[1]
    res = []
    if business_tobepredict not in dic.keys():
        res.append((4,('no_busi', 'no_user'), (user_tobepredict, business_tobepredict)))
        return res
    if user_tobepredict not in user_busi_dic.keys():
        res.append((4, ('no_busi', 'no_user'), (user_tobepredict, business_tobepredict)))
        return res
    setofuser_tobepredict = set(dic[business_tobepredict].keys())
    for business in user_busi_dic[user_tobepredict]:
        listofuser_other = set(dic[business].keys())
        co_rated_user = setofuser_tobepredict.intersection(listofuser_other)
        #if len(co_rated_user) > 0:
        #similarity = 0
        similarity = computeSimilarity(co_rated_user, dic, business_tobepredict, business, averageBusi_rdd)
        if similarity > 0:
            res.append((similarity, (user_tobepredict, business), (user_tobepredict, business_tobepredict)))
    if len(res) == 0:
        res.append((4,('no_busi', 'isuser'), (user_tobepredict, business_tobepredict)))
    res = sorted(res, key = lambda x: x[0], reverse=True)
    return res

def getprediction(x, dic):
    numerator = 0
    denominator = 0
    for row in x:
        if row[1][0] == 'no_busi':
            return row[0]
        if row[1][0] in dic[row[1][1]].keys():
            numerator += dic[row[1][1]][row[1][0]]*row[0]
            denominator += row[0]
    if numerator == 0 or denominator == 0:
        return 4
    res = numerator / denominator
    return res

def filterout(x,n):
    lis = []
    if n > len(x):
        n = len(x)
    for i in range(0,n):
        lis.append(x[i])
    return lis

#input_file_path = "train_slides.csv"
#train_data = "self_val_in.csv"
file_path = sys.argv[1]
input_file_path = file_path + "/yelp_train.csv"
train_data = sys.argv[2]
output_file_path = sys.argv[3]

first_line = sc.textFile(input_file_path).first()
rdd = sc.textFile(input_file_path).filter(lambda row: row != first_line)
dic_rdd = rdd.map(lambda row: (str(row.split(',')[1]), (str(row.split(',')[0]), float(row.split(',')[2])))).groupByKey().mapValues(dict)
averageBusi_rdd = rdd.map(lambda row: (str(row.split(',')[1]), float(row.split(',')[2]))).groupByKey().mapValues(list).map(lambda x: (x[0], sum(x[1])/len(x[1])))
averageUser_rdd = rdd.map(lambda row: (str(row.split(',')[0]), float(row.split(',')[2]))).groupByKey().mapValues(list).map(lambda x: (x[0], sum(x[1])/len(x[1])))

averageBusi_dic = dict(averageBusi_rdd.collect())
averageUser_dic = dict(averageUser_rdd.collect())

user_busi_rdd = rdd.map(lambda row: (str(row.split(',')[0]), (str(row.split(',')[1])))).groupByKey().mapValues(list)
user_busi_dic = dict(user_busi_rdd.collect())

dic = dict(dic_rdd.collect())

N = 10 # number of neighborhood item
first_line_train = sc.textFile(train_data).first()
training = sc.textFile(train_data).filter(lambda row: row != first_line)\
    .map(lambda row: (str(row.split(',')[1]), str(row.split(',')[0]))).map(lambda x: getSimilar(x, dic, user_busi_dic, averageBusi_dic, averageUser_dic))\
    .map(lambda x: filterout(x, N))

predict = training.map(lambda x: getprediction(x, dic)).collect()

predict_1 = np.array(predict) # data from task2_1


######## from task 1

###### task2
def getTrainingData(row,user_dic,business_dic, review_train):
    user = row[0]
    business = row[1]
    star = 4
    dates = 20111107
    if user not in user_dic.keys(): # testing data is not in training dataset.
        return [user, business, 10,3,5,4,10,3,0] # reasonable guess
    if business not in business_dic.keys():
        return [user, business, 10,3,5,4,10,3,0]
    if user in review_train.keys():
        star = review_train[user][0]
        dates = float(review_train[user][1].replace("-", ""))
    reviewcount = user_dic[user][0]
    averagestar = user_dic[user][1]
    fans = user_dic[user][2]
    yelping_since = float(user_dic[user][3].replace("-", ""))
    review = business_dic[business][0]
    stars = business_dic[business][1]
    hash_city = business_dic[business][2]
    return [user, business, reviewcount, averagestar, fans, yelping_since, review, stars, hash_city, star, dates]


input_user = file_path + "/user.json"
input_business = file_path + "/business.json"
input_review_train = file_path + "/review_train.json"
first_line_train = sc.textFile(input_file_path).first()
first_line_test = sc.textFile(train_data).first()
regressor = xgb.XGBRegressor(objective='reg:linear')

train = sc.textFile(input_file_path).filter(lambda row: row != first_line_train)\
    .map(lambda row: (str(row.split(',')[0]), str(row.split(',')[1]), str(row.split(',')[2]))).cache()
user = sc.textFile(input_user).map(json.loads).\
    map(lambda row: (str(row["user_id"]), (float(row["review_count"]), float(row["average_stars"]), float(row["fans"]), (row["yelping_since"])))).cache()
business = sc.textFile(input_business).\
    map(json.loads).map(lambda row: (str(row["business_id"]), (float(row["review_count"]), float(row["stars"]), float(hash(row["city"]))))).cache() # get as many data as possible
review_train = sc.textFile(input_review_train).map(json.loads).\
    map(lambda row: (str(row["user_id"]), (float(row["stars"]), (row["date"])))).cache()
user_dic = dict(user.collect())
business_dic = dict(business.collect())
review_train = dict(review_train.collect())
train_x = train.map(lambda row: getTrainingData(row,user_dic,business_dic,review_train)).collect()
train_y = train.map(lambda x : x[2]).collect()

array_train_x = np.array(train_x)
array_train_y = np.array(train_y)

float_train_x = np.array(array_train_x[:, 2:8])
float_train_y = np.array(array_train_y)


regressor.fit(float_train_x, float_train_y)


test = sc.textFile(train_data).filter(lambda row: row != first_line_test)\
    .map(lambda row: (str(row.split(',')[0]), str(row.split(',')[1]))).cache()

test_data_x = test.map(lambda row: getTrainingData(row,user_dic,business_dic,review_train)).collect()


array_test_x = np.array(test_data_x)
#array_test_y = np.array(test_data_y)
#print(array_test_x)
float_test_x = np.array(array_test_x[:, 2:8])
#print(float_test_x)


predict_2 = regressor.predict(float_test_x) # data from task2_2

predict_3 = 0.001 * predict_1 + 0.999 * predict_2 # weighted average

#print("res " + str(result))
#rmse = np.sqrt(MSE(test_data_y, predict_3))
#print("RMSE : % f" %(rmse))
with open(output_file_path, 'w') as output:
    output.write("user_id, business_id, prediction\n")
    for i in range(len(array_test_x)):
        output.write(str(array_test_x[i][0]) + "," + str(array_test_x[i][1])+","+str(predict_3[i])+"\n")
print("Duration: " + str(time.time()-start))