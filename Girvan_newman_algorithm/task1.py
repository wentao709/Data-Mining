# Wentao Zhou
# DSCI 553, HW4, task 2, Spring 2021

import os
import sys
from pyspark.sql import SQLContext
from pyspark import SparkContext
from graphframes import *
import time
start = time.time()
os.environ["PYSPARK_SUBMIT_ARGS"] = (
"--packages graphframes:graphframes:0.6.0-spark2.3-s_2.11")
sc = SparkContext('local[*]', 'task1')
sc.setLogLevel("ERROR")
sqlContext = SQLContext(sc)
threshold = int(sys.argv[1])
#input_file_path = "ub_test.csv"
input_file_path = sys.argv[2]
output_file_path = sys.argv[3]
first_line = sc.textFile(input_file_path).first()
rdd = sc.textFile(input_file_path).filter(lambda row: row != first_line)\
    .map(lambda row: (str(row.split(',')[0]), str(row.split(',')[1]))).groupByKey().mapValues(set)
user_busi = dict(rdd.collect())

def connectNodewithEdge(user_busi):
    edges_dic = dict()
    for user1 in user_busi.keys():
        for user2 in user_busi.keys():
            if user1 != user2:
                common_busi = user_busi[user1].intersection(user_busi[user2])
                if len(common_busi) >= threshold:
                    if user1 not in edges_dic:
                        edges_dic[user1] = [user2]
                    else:
                        edges_dic[user1].append(user2)
    return edges_dic
edge_dic = connectNodewithEdge(user_busi)
node_list = []
edge_list = []
for key in edge_dic.keys():
  node_list.append((key,))
  for value in edge_dic[key]:
    pair = sorted([key, value])
    edge_list.append((pair[0], pair[1]))

vertices = sqlContext.createDataFrame(node_list, ["id"])
edges = sqlContext.createDataFrame(edge_list, ["src", "dst"])
#node_list = [('A',), ('C',), ('E',), ('G',), ('B',), ('D',), ('F',)]
#vertices = sqlContext.createDataFrame(node_list, ["id"])


g = GraphFrame(vertices, edges)

communities = g.labelPropagation(maxIter=5).collect()
lis = []
dic = {}
for community in communities:
    community = tuple(community)
    if community[1] not in dic:
        dic[community[1]] = [community[0]]
    else:
        dic[community[1]].append(community[0])
res = []
#print(dic)
for lists in dic.keys():
    res.append(sorted(dic[lists]))
res = sorted(res, key=lambda node: (len(node), node[0]))
#print(res)

with open(output_file_path, 'w') as output:
    for community in res:
        for i in range(len(community)-1):
            output.write("'" + str(community[i]) + "', ")
        output.write("'" + str(community[len(community)-1]) + "'\n")