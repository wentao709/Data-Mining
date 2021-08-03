# Wentao Zhou
# DSCI 553 - hw1, task3, spring 2021
# spark warm-up practice

from pyspark import SparkContext
import json
import time
import sys
sc = SparkContext('local[*]', 'task3')

input_file_path = sys.argv[1]
input_file_path2 = sys.argv[2]
output1 = sys.argv[3]
output2 = sys.argv[4]
test = sc.textFile(input_file_path).map(json.loads).map(lambda dic: (dic["business_id"], dic["stars"])).cache()
business = sc.textFile(input_file_path2).map(json.loads).map(lambda dic: (dic["business_id"], dic["city"])).cache()

# ======================= exe time of sort by python ===========================
python_start = time.time()
joined = business.join(test)
joined = joined.map(lambda x: x[1]).groupByKey().mapValues(list)\
    .mapValues(lambda x: sum(x)/len(x))
cities = sorted(joined.collect(), key=lambda x: (-x[1], x[0]))
topten = cities[:10]
python_end = time.time()
python_exe_time = python_end - python_start
with open(output1, 'w') as output:
    output.write("city,stars\n")
    for city in cities:
        output.write(str(city[0]) + "," + str(city[1]) + "\n")
output.close()
for city in topten:
    print(city[0])
# ======================= exe time of sort by python ===========================


# ======================= exe time of sort by spark ===========================
spark_start = time.time()
joined = business.join(test)
joined = joined.map(lambda x: x[1]).groupByKey().mapValues(list)\
    .mapValues(lambda x: sum(x)/len(x))
cities = joined.takeOrdered(10, key=lambda x: (-x[1], x[0]))
spark_end = time.time()
spark_exe_time = spark_end - spark_start
for city in cities:
    print(city[0])
dic = {}
dic["m1"] = python_exe_time
dic["m2"] = spark_exe_time
with open(output2, 'w') as output2:
    json.dump(dic, output2)
output2.close()
# ======================= exe time of sort by spark ===========================