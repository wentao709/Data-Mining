# Wentao Zhou
# DSCI 553 - hw1, task2, spring 2021
# spark warm-up practice

from pyspark import SparkContext
import json
import os
import time
import sys

sc = SparkContext('local[*]', 'task2')
input_file_path = sys.argv[1]
output = sys.argv[2]
input_partition = int(sys.argv[3])
textRDD = sc.textFile(input_file_path).map(json.loads)
textRDD =  textRDD.map(lambda row: row["business_id"]).cache()
# ======================= exe time of default partition function ===========================
startTime = time.time()
toptenbusiness = textRDD\
        .map(lambda row: (row, 1)).reduceByKey(lambda a, b: a+b)
topten = toptenbusiness.takeOrdered(10, key=lambda x: (-x[1], x[0]))
endTime = time.time()
exe_time = endTime - startTime

numofpartition = toptenbusiness.getNumPartitions()
numitem = toptenbusiness.glom().map(len).collect()
# ======================= exe time of default partition function ===========================

# ======================= exe time of custom partition function ===========================
startTime = time.time()
toptenbusiness = textRDD\
        .map(lambda row: [row, 1]).reduceByKey(lambda a, b: a+b, input_partition, lambda id: hash(id))
topten = toptenbusiness.takeOrdered(10, key=lambda x: (-x[1], x[0]))
endTime = time.time()
my_exe_time = endTime - startTime
my_numofpartition = toptenbusiness.getNumPartitions()
my_numitem = toptenbusiness.glom().map(len).collect()
# ======================= exe time of custom partition function ===========================

res = {}
res["default"] = {
    "n_partition": numofpartition,
    "n_items": numitem,
    "exe_time": exe_time
}
res["customized"] = {
    "n_partition": my_numofpartition,
    "n_items": my_numitem,
    "exe_time": my_exe_time
}
with open(output, 'w') as output:
    json.dump(res, output)
output.close()