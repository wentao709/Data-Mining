# Wentao Zhou
# HW6, DSCI 553 - data mining, Spring 2021
# BFR algorithm for clustering big data

import sys
import random
from copy import deepcopy

from sklearn.cluster import KMeans
import numpy as np

# randomly load data
class BlackBox:
    def ask(file):
        lines = open(file,'r').readlines()
        random.shuffle(lines)
        return lines

input_file_path = sys.argv[1]
n_cluster = int(sys.argv[2])
output_file_path = sys.argv[3]

bx = BlackBox
loaded_data = bx.ask(input_file_path)

# process the data to the desired format
def preprocessing(loaded_data):
    res = []
    for row in loaded_data:
        lis = row.split(',')
        res.append(lis)
    return res

# update the stats of input clusters
# return dic: key: cluster index, value: summary of each cluster
def updatesummary(DS_dic):
    DS_sum_dic = {}
    for key in DS_dic: # for each cluster
        inputs = np.array(DS_dic[key]) # list of points in the current cluster
        inputs = inputs[:, 2:]
        index = list(np.array(DS_dic[key])[:, 0])
        N = len(DS_dic[key]) # number of points in the cluster
        SUM = np.sum(inputs, axis=0)
        SUMSQ = np.sum(np.square(inputs), axis=0)
        centroid = SUM / N
        variance = np.subtract((SUMSQ / N), np.square(SUM / N))
        standard_dev = np.sqrt(variance)
        DS_sum_dic[key] = [N, SUM, SUMSQ, centroid, standard_dev, index]
    return DS_sum_dic

preprocessed = preprocessing(loaded_data) # original data
# step 1
divider = int(len(preprocessed) * 0.2) # 20% percent of data
partial_data = preprocessed[:divider]

np_loaded_data = np.array(partial_data, dtype='float')

# step 2
kmeans = KMeans(n_clusters=10*n_cluster).fit(np_loaded_data[:, 2:])
global_RS = [] # retain set
cluster_dic = {} # key: cluster_index, value: points
cluster_index = kmeans.labels_ # assign numbers to clusters
for i in range(len(cluster_index)):
    if cluster_index[i] not in cluster_dic: # new cluster
        cluster_dic[cluster_index[i]] = [np_loaded_data[i]]
    else:
        cluster_dic[cluster_index[i]].append(np_loaded_data[i]) # add new points to the clusters
# step 3
rest_data = []
for key in cluster_dic.keys():
    if len(cluster_dic[key]) == 1:
        global_RS.append(cluster_dic[key][0])
    else:
        for point in cluster_dic[key]:
            rest_data.append(point) # the data that doesn't add to RS
# step 4

rest_data_np = np.array(rest_data, dtype='float')
kmeans2 = KMeans(n_clusters=n_cluster).fit(rest_data_np[:, 2:])
global_DS_dic = {} # key: cluster index, value: points
# step 5

cluster_index = kmeans2.labels_
for i in range(len(cluster_index)):
    if cluster_index[i] not in global_DS_dic: # new cluster
        global_DS_dic[cluster_index[i]] = [rest_data_np[i]]
    else:
        global_DS_dic[cluster_index[i]].append(rest_data_np[i])
global_DS_sum_dic = updatesummary(global_DS_dic)
#return CS and rs: key:
def generageCS_RS(kmeans, RS_np): # for step 6 and 11
    CS_dic = {}
    cluster_assign = {} # key: cluster index, value: points
    newRS = []
    cluster_index = kmeans.labels_
    for i in range(len(cluster_index)):
        if cluster_index[i] not in cluster_assign:
            cluster_assign[cluster_index[i]] = [RS_np[i]]
        else:
            cluster_assign[cluster_index[i]].append(RS_np[i])
    for key, value in cluster_assign.items():
        if len(value) == 1:
            newRS.append(value[0])
        elif len(value) > 1:
            if key not in CS_dic:
                CS_dic[key] = value
            else:
                CS_dic[key].append(value)
    return CS_dic, newRS

global_CS_sum_dic = {}
global_CS_dic = {}
# step 6

if len(global_RS) > (10 * n_cluster): # RS may not be large enough to assign to the given number of clusters
    np_glo_RS = np.array(global_RS)
    nps_glo_RS = np_glo_RS[:, 2:]
    kmeans = KMeans(n_clusters=10*n_cluster).fit(nps_glo_RS) # change the cluster later!!!!!!!
    global_CS_dic, global_RS = generageCS_RS(kmeans, np_glo_RS)
    global_CS_sum_dic = updatesummary(global_CS_dic)

# add new point to current cluster and summarize
def updatecluster(key, newpoint, cluster_summary_dic, point_index, source):
    cluster_summary_dic[key][0] += 1
    cluster_summary_dic[key][1] += newpoint
    cluster_summary_dic[key][2] += np.square(newpoint)
    N = cluster_summary_dic[key][0]
    SUM = cluster_summary_dic[key][1]
    SUMSQ = cluster_summary_dic[key][2]
    centroid = SUM / N
    variance = np.subtract((SUMSQ / N), np.square(SUM / N))
    standard_dev = np.sqrt(variance)
    cluster_summary_dic[key][3] = centroid
    cluster_summary_dic[key][4] = standard_dev
    cluster_summary_dic[key][5].append(point_index)

# summary the CS after merging
def summaryMergedCS(CS_merged_dic, CS_merged):
    mergedCS_sum_dic = {}
    for key in CS_merged_dic.keys():
        nplist = []
        klist = CS_merged_dic[key]
        for k in klist:
            nplist.append(CS_merged[k])
        nplist = np.array(nplist, dtype='object')
        l = deepcopy(nplist)
        N = np.sum(nplist[:, 0])
        SUM = np.sum(nplist[:, 1])
        SUMSQ = np.sum(nplist[:, 2])
        centroid = SUM / N
        variance = np.subtract((SUMSQ / N), np.square(SUM / N))
        standard_dev = np.sqrt(variance)
        index = []
        for ind in l[:, 5]:
            index += list(ind)
        mergedCS_sum_dic[key] = [N,SUM,SUMSQ,centroid,standard_dev,index]
    return mergedCS_sum_dic

# CS_merged: each key map to one cluster
def mergeCS(CS_merged, CS_merged_dic, threshold):
    key = 0
    list_keys = list(CS_merged.keys())
    for i in range(0, len(list_keys)):
        flag2 = False
        key1 = list_keys[i]
        for j in range(i+1, len(list_keys)):
            key2 = list_keys[j]
            y = np.subtract(CS_merged[key1][3], CS_merged[key2][3]) / CS_merged[key2][4]
            sumofsquareofy = np.sum(np.square(y), axis=0)
            maha_distance = np.sqrt(sumofsquareofy)
            if maha_distance < threshold: # then merge
                flag2 = True # point i is merged
                flag = False
                for keys, values in CS_merged_dic.items(): # key: cluster index, value: summary
                    if key1 in values and key2 not in values:
                        CS_merged_dic[keys].add(key2)
                        flag = True
                    elif key2 in values and key1 not in values:
                        CS_merged_dic[keys].add(key1)
                        flag = True
                    elif key1 in values and key2 in values:
                        flag = True
                if not flag:
                    CS_merged_dic[key] = {key1, key2}
                    key += 1
        if not flag2: # key1 doesn't merge at this iteration
            flag = False
            for keys, values in CS_merged_dic.items():
                if key1 in values: # key1 already merged with some clusters
                    flag = True
            if not flag:
                CS_merged_dic[key] = {key1}
                key += 1
    mergedCS_sum_dic = summaryMergedCS(CS_merged_dic, CS_merged)

    return mergedCS_sum_dic

# for output
discard_points = 0
cs_cluster_num = len(global_CS_sum_dic.keys())
cs_points = 0
for values in global_DS_sum_dic.values():
    discard_points += values[0]
for values in global_CS_sum_dic.values():
    cs_points += values[0]
rd_points = len(global_RS)
with open(output_file_path, 'w') as output:
    output.write("The intermediate results:\n")
    output.write("Round 1: " + str(discard_points) + "," + str(cs_cluster_num) + "," + str(cs_points) + "," + str(rd_points) + "\n")
# for output

# prepare for step 7
merge_key = 0

global_CS_merged_dic = {}


def merge_DS_CS(DS_sum_dic, key, cs):
    DS_sum_dic[key][0] += cs[0]
    DS_sum_dic[key][1] += cs[1]
    DS_sum_dic[key][2] += cs[2]
    N = DS_sum_dic[key][0]
    SUM = DS_sum_dic[key][1]
    SUMSQ = DS_sum_dic[key][2]
    centroid = SUM / N
    variance = np.subtract((SUMSQ / N), np.square(SUM / N))
    standard_dev = np.sqrt(variance)
    DS_sum_dic[key][3] = centroid
    DS_sum_dic[key][4] = standard_dev
    DS_sum_dic[key][5] += list(cs[5])

def mergeCS_DS(CS_sum_dic, DS_sum_dic):
    CSlist = list(CS_sum_dic.values())
    for cs in CSlist:
        for key, value in DS_sum_dic.items():
            y = np.subtract(cs[3], value[3]) / value[4]
            sumofsquareofy = np.sum(np.square(y), axis=0)
            maha_distance = np.sqrt(sumofsquareofy)
            lis.append((key, maha_distance))
        cluster_dis = sorted(lis, key=lambda key: key[1])
        if cluster_dis[0][1] < threshold:
            merge_DS_CS(DS_sum_dic, cluster_dis[0][0], cs)

for round in range(1,5):
    # step7
    partial_data = []
    if round < 4:
        partial_data = preprocessed[round * divider: (round + 1) * divider]
    else:
        partial_data = preprocessed[round * divider:]
    # step8
    np_partial_data = np.array(partial_data, dtype='float')
    np_partial_data = np_partial_data
    threshold = 2 * np.sqrt(len(np_partial_data[:, 2:][0]))  # the threshold
    leftover_points = []
    if len(global_DS_sum_dic) == 0: # no DS, skip step 8
        leftover_points = np_partial_data
    else:
        for points in np_partial_data: # for each point
            point = points[2:]
            point_index = points[0]
            lis = []
            for key, value in global_DS_sum_dic.items(): # key in the cluster index, value is the summary of points
               y = np.subtract(point, value[3]) / value[4]
               sumofsquareofy = np.sum(np.square(y), axis = 0)
               maha_distance = np.sqrt(sumofsquareofy)
               lis.append((key, maha_distance))
            cluster_dis = sorted(lis, key=lambda key: key[1])
            if cluster_dis[0][1] < threshold:
                updatecluster(cluster_dis[0][0], point, global_DS_sum_dic, point_index, 1)
            else:
                leftover_points.append(points)
    # step 9
    final_leftover = []
    if (len(global_CS_sum_dic) == 0):  # no CS, skip step 8
        final_leftover = leftover_points
    else:
        for points in leftover_points:
            point = points[2:]
            point_index = points[0]
            lis = []
            for key, value in global_CS_sum_dic.items():
                y = np.subtract(point, value[3]) / value[4]
                sumofsquareofy = np.sum(np.square(y), axis=0)
                maha_distance = np.sqrt(sumofsquareofy)
                lis.append((key, maha_distance))
            cluster_dis = sorted(lis, key=lambda key: key[1])
            if cluster_dis[0][1] < threshold:
                updatecluster(cluster_dis[0][0], point, global_CS_sum_dic, point_index,2 )
            else:
                final_leftover.append(points)
    global_CS_merged = {} # ???? # collect all the cs clusters and later use it to merge cs
    for values in global_CS_sum_dic.values():
        # global_CS_merged.append(values) # collecting CS cluster summary
        global_CS_merged[merge_key] = values
        merge_key += 1
    #
    # step 10
    for point in final_leftover:
        global_RS.append(point)
    # step 11
    new_CS_dic = {}
    new_CS_sum_dic = {}
    if len(global_RS) > 10 * n_cluster:
        np_glo_RS = np.array(global_RS)
        nps_glo_RS = np_glo_RS[:, 2:]
        kmeans = KMeans(n_clusters=10*n_cluster).fit(nps_glo_RS)
        new_CS_dic, global_RS = generageCS_RS(kmeans, np_glo_RS)

        new_CS_sum_dic = updatesummary(new_CS_dic)
    for values in new_CS_sum_dic.values():
        global_CS_merged[merge_key] = values
        merge_key += 1
    # step 12
    global_CS_merged_dic = mergeCS(global_CS_merged, {}, threshold)
    global_CS_sum_dic = global_CS_merged_dic
    merge_key = 0

    #for output
    discard_points = 0
    cs_cluster_num = len(global_CS_sum_dic.keys())
    cs_points = 0
    for values in global_DS_sum_dic.values():
        discard_points += values[0]
    for values in global_CS_sum_dic.values():
        cs_points += values[0]
    rd_points = len(global_RS)
    with open(output_file_path, 'a') as output:
        output.write(
            "Round " + str(round+1) + ": " + str(discard_points) + "," + str(cs_cluster_num) + "," + str(cs_points) + "," + str(rd_points) + "\n")
    if round == 4:
        mergeCS_DS(global_CS_sum_dic, global_DS_sum_dic)

res = []
for key, values in global_DS_sum_dic.items():
    for value in values[5]:
        res.append((value, key))
for point in global_RS:
    res.append((point[0], -1))
res = sorted(res)
with open(output_file_path, 'a') as output:
    output.write("\n")
    output.write("The clustering results:\n")
    for element in res:
        output.write(str(int(element[0])) + "," + str(str(element[1])) + "\n")

