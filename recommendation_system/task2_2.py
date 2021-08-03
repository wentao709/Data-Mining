# Wentao Zhou
# DSCI 553 - data mining, Spring 2021, HW3, task2_2
# recommendation system: model-based CF

from pyspark import SparkContext
import json
import sys
import math
import time
import xgboost as xgb
from sklearn.metrics import mean_squared_error as MSE
import numpy as np

# get training data, I'm trying to get as many featuers data as I can.
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



start = time.time()
file_path = sys.argv[1]
input_train = file_path + "/yelp_train.csv"
input_user = file_path + "/user.json"
input_business = file_path + "/business.json"
input_review_train = file_path + "/review_train.json"
input_test = sys.argv[2]
output_file = sys.argv[3]
sc = SparkContext('local[*]', 'task2_2')
sc.setLogLevel("ERROR")
first_line_train = sc.textFile(input_train).first()
first_line_test = sc.textFile(input_test).first()
regressor = xgb.XGBRegressor(objective='reg:linear')
#-------------------training stage--------------------#
train = sc.textFile(input_train).filter(lambda row: row != first_line_train)\
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
train_x = train.map(lambda row: getTrainingData(row,user_dic,business_dic, review_train)).collect()
train_y = train.map(lambda x : x[2]).collect()

array_train_x = np.array(train_x) # training data x
array_train_y = np.array(train_y) # training data y

float_train_x = np.array(array_train_x[:, 2:8])
float_train_y = np.array(array_train_y)


regressor.fit(float_train_x, float_train_y)
#-------------------training stage--------------------#

#-------------------testing stage--------------------#
test = sc.textFile(input_test).filter(lambda row: row != first_line_test)\
    .map(lambda row: (str(row.split(',')[0]), str(row.split(',')[1]))).cache()

test_data_x = test.map(lambda row: getTrainingData(row,user_dic,business_dic, review_train)).collect()


array_test_x = np.array(test_data_x)
#print(array_test_x)
float_test_x = np.array(array_test_x[:, 2:8])
#print(float_test_x)


predict = regressor.predict(float_test_x)

with open(output_file, 'w') as output:
    output.write("user_id, business_id, prediction\n")
    for i in range(len(array_test_x)):
        output.write(str(array_test_x[i][0]) + "," + str(array_test_x[i][1])+","+str(predict[i])+"\n")
#rmse = np.sqrt(MSE(predict, float_test_y))
#print("RMSE : % f" %(rmse))
print("Duration: " + str(time.time()-start))