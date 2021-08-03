# Wentao Zhou
# DSCI 553, HW4, task 2, Spring 2021
from copy import deepcopy

from pyspark import SparkContext
import sys
import time

start = time.time()
sc = SparkContext('local[*]', 'task2')
sc.setLogLevel("ERROR")

threshold = int(sys.argv[1])
input_file_path = sys.argv[2]
output_file_path = sys.argv[3]
output_file_path2 = sys.argv[4]
first_line = sc.textFile(input_file_path).first()
rdd = sc.textFile(input_file_path).filter(lambda row: row != first_line) \
    .map(lambda row: (str(row.split(',')[0]), str(row.split(',')[1]))).groupByKey().mapValues(set)
user_busi = dict(rdd.collect())


# get the graph
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

origin_graph = deepcopy(edge_dic)


# calculate the betweenness of all edges
def betweenCal(user_root, edge_dic):
    if user_root not in edge_dic:
        return []
    queue = []
    visited = set()
    visited.add(user_root)
    queue.append(user_root)
    node_value_dic = {}
    node_value_dic[user_root] = 1  # shortest path, others nodes initialized to 0
    nextlevel = []
    parent_dic = {}
    parent_dic[user_root] = []
    level_list = []
    newnode_value_dic = {}
    dist = {}
    dist[user_root] = 0  # other nodes initialized to inf
    # BFS
    while queue:
        node = queue.pop(0)
        for n in edge_dic[node]:
            if n not in dist or dist[n] > dist[node]:
                if n not in node_value_dic:  # haven't visited, so its dist is inf
                    nextlevel.append(n)
                    visited.add(n)
                    parent_dic[n] = [node]
                    node_value_dic[n] = node_value_dic[node]
                    dist[n] = dist[node] + 1
                    newnode_value_dic[n] = 1  # for step 3
                else:
                    visited.add(n)
                    parent_dic[n].append(node)
                    if dist[n] > dist[node] + 1:
                        dist[n] = dist[node] + 1
                        node_value_dic[n] = node_value_dic[node]
                    elif dist[n] == dist[node] + 1:
                        node_value_dic[n] += node_value_dic[node]
        if not queue and not nextlevel:
            break
        if not queue:
            queue = nextlevel.copy()
            level_list.insert(0, nextlevel.copy())
            nextlevel = []

    # step 3 of the algorithm
    res = []
    for i in range(len(level_list)):
        for node in level_list[i]:
            for parent in parent_dic[node]:
                edge_name = sorted([parent, node])[0] + " " + sorted([parent, node])[1]
                edge_value = (edge_name, newnode_value_dic[node] * float(node_value_dic[parent] / node_value_dic[node]))
                res.append(edge_value)
                if parent != user_root:
                    newnode_value_dic[parent] += newnode_value_dic[node] * float(
                        node_value_dic[parent] / node_value_dic[node])
    return res


# betweenness
edge_value = rdd.map(lambda x: x[0]).map(lambda user: betweenCal(user, edge_dic)).filter(lambda x: len(x) > 0) \
    .flatMap(lambda x: x).groupByKey().mapValues(sum).map(lambda x: (x[0].split(" "), x[1] / 2)) \
    .sortBy(lambda x: (-x[1], x[0][0])).collect()

with open(output_file_path, 'w') as output:
    for row in edge_value:
        output.write("('" + str(row[0][0]) + "', '" + str(row[0][1]) + "')," + str(round(row[1], 5)) + "\n")

with open(output_file_path2, 'w') as output:
    output.write("communities\n")