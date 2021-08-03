# Wentao Zhou
# DSCI 553 - hw2, task2, spring 2021
# find frequent items using SON algorithm

from pyspark import SparkContext
import json
import os
import pyspark
import time
import sys
import itertools
from collections import defaultdict
from itertools import combinations
start = time.time()
sc = SparkContext('local[*]', 'task2')
filter = int(sys.argv[1])
s = int(sys.argv[2])
input_file_path = sys.argv[3]
output_file_path = sys.argv[4]
outputfile = "myfile.csv" # file having preprosessing data
combi_size = 2
# the exactly same algorithm from task1, check the comments there
def combination(row,n):
    l = row
    return [(v) for v in itertools.combinations(l, n)]

# the exactly same algorithm from task1, check the comments there
def APriori(partition, numpartition):
    partition = tuple(partition)
    dic = {}
    candidate = []
    p = 1 / numpartition
    for tup in partition:
        for element in tup:
            if element not in dic:
                dic[element] = 1
            else:
                dic[element] += 1
            if dic[element] == int(s // numpartition):
                sets = set()
                sets.add(element)
                if sets not in candidate:
                    candidate.append(sets)
    length = len(candidate)
    combi = 2
    all_candidate = []
    while length > 0:
        all_candidate.append(candidate)
        all_possible = combination(candidate, combi_size)
        candidate = []
        check_exist = set()
        for item in all_possible:
            iset = set()
            for it in item[0]:
                iset.add(it)
            for it in item[1]:
                iset.add(it)
            item = iset
            if (combi == len(item)):
                if tuple(sorted(tuple(item))) not in check_exist:
                    check_exist.add(tuple(sorted(tuple(item))))
                    for tup in partition:
                        if (item).issubset(tup):
                            itemtuple = tuple(sorted(tuple(item)))
                            if itemtuple in dic:
                                dic[itemtuple] += 1
                            else:
                                dic[itemtuple] = 1
                            if (dic[itemtuple] == int(s // numpartition)):
                                candidate.append(item)
                                break
        length = len(candidate)
        combi += 1
    return all_candidate

first_line = sc.textFile(input_file_path).first()
textRDD = sc.textFile(input_file_path).filter(lambda row: row!=first_line)\
    .map(lambda row: (str(row.split("\"")[1]) + "-" + str(row.split("\"")[3]),str(int(row.split("\"")[11])))).collect() # prerosessing data
with open(outputfile, 'w') as output:
    output.write("DATE-CUSTOMER_ID, PRODUCT_ID\n")
    for row in textRDD:
        output.write(str(row[0]) + "," + str(row[1]) + "\n") # generate intermediate data

textRDD = sc.textFile(outputfile).filter(lambda row: row != "DATE-CUSTOMER_ID, PRODUCT_ID")\
    .map(lambda row: (str(row.split(',')[0]), str(row.split(',')[1]))).groupByKey().mapValues(set).map(lambda x: x[1])

baskets = textRDD.filter(lambda row: len(row) > filter).map(lambda row: set(row)) # filter out items that's going to be processing
numpartition = baskets.getNumPartitions()
candidate= baskets.mapPartitions(lambda partition: APriori(partition, numpartition))\
    .flatMap(lambda x: x).map(lambda x: tuple(sorted(tuple(x)))).distinct().collect()

candidate1 = (sorted(list((candidate)), key=lambda x: (len(x))))

# the exactly same algorithm from task1, check the comments there
def phase2(basket, candidate1):
    basket = tuple(basket)
    res = []
    for element in candidate1:
        set_ele = set(element)
        for tuples in basket:
            if set_ele.issubset(tuples):
                res.append((tuple(sorted(tuple(set_ele))),1))
    return res

# the exactly same algorithm from task1, check the comments theree
def write_output(candidate1):
    length = len(candidate1)
    tuple_length = candidate1[0]
    tuple_list = []
    for i in range(length):
        if tuple_length == len(candidate1[i]):
            tuple_list.append(candidate1[i])
        else:
            tuple_list.sort()
            if tuple_length == 1:
                with open(output_file_path, 'a') as output:
                    for j in range(len(tuple_list)):
                        if j < len(tuple_list) - 1:
                            output.write("(\'" + str(tuple_list[j][0]) + "\')" + ",")
                        else:
                            output.write("(\'" + str(tuple_list[j][0]) + "\')" + "\n")
                            output.write("\n")
            else:
                with open(output_file_path, 'a') as output:
                    for j in range(len(tuple_list)):
                        if j < len(tuple_list) - 1:
                            output.write(str(tuple_list[j]) + ",")
                        else:
                            output.write(str(tuple_list[j]) + "\n")
                            output.write("\n")
            tuple_list = []
            tuple_length = len(candidate1[i])
            tuple_list.append(candidate1[i])
    tuple_list.sort()
    if tuple_length == 1:
        with open(output_file_path, 'a') as output:
            for j in range(len(tuple_list)):
                if j < len(tuple_list) - 1:
                    output.write("(\'" + str(tuple_list[j][0]) + "\')" + ",")
                else:
                    output.write("(\'" + str(tuple_list[j][0]) + "\')" + "\n")
                    output.write("\n")
    else:
        with open(output_file_path, 'a') as output:
            for j in range(len(tuple_list)):
                if j < len(tuple_list) - 1:
                    output.write(str(tuple_list[j]) + ",")
                else:
                    output.write(str(tuple_list[j]) + "\n")
                    output.write("\n")
with open(output_file_path, 'w') as output:
    output.write("Candidates:\n")
write_output(candidate1)
candidate2 = baskets.mapPartitions(lambda partition: phase2(partition, candidate1)) \
    .reduceByKey(lambda a, b: a + b).filter(lambda x: x[1] >= s).map(lambda x: x[0]).collect()
candidate2 = sorted(list((candidate2)), key=lambda x: (len(x)))

with open(output_file_path, 'a') as output:
    output.write("Frequent Itemsets:\n")
write_output(candidate2)
end = time.time()
print("Duration: " + str(end - start))
