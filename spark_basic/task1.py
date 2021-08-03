# Wentao Zhou
# DSCI 553 - hw1, task1, spring 2021
# spark warm-up practice

from pyspark import SparkContext
import json
import sys

sc = SparkContext('local[*]', 'task1')
input_file_path = sys.argv[1]
output = sys.argv[2]
textRDD = sc.textFile(input_file_path).map(json.loads)\
    .map(lambda row: (row["business_id"], row["user_id"], row["date"])).cache()
totalnumReview = textRDD.count() #q1
numreview = textRDD.filter(lambda row: int(row[2][0: 4]) == 2018).count() #q2
#textRDD = textRDD.filter(lambda dic: len(dic["text"]) > 0)
numofdistinctuser = textRDD\
    .map(lambda row: row[1]).distinct().count()  #q3
toptenusers = textRDD\
    .map(lambda row: [row[1], 1]).reduceByKey(lambda a, b: a+b).takeOrdered(10, key=lambda x: (-x[1], x[0])) #Q4
numdistinctbusineess = textRDD\
    .map(lambda row: row[0]).distinct().count() #q5
toptenbusiness = textRDD\
    .map(lambda row: [row[0], 1]).reduceByKey(lambda a, b: a+b).takeOrdered(10, key=lambda x: (-x[1], x[0])) #q6

res_dic = {"n_review": totalnumReview,
            "n_review_2018": numreview,
            "n_user": numofdistinctuser,
            "top10_user": toptenusers,
            "n_business": numdistinctbusineess,
            "top10_business": toptenbusiness}
with open(output, 'w') as output:
    json.dump(res_dic, output)
output.close()