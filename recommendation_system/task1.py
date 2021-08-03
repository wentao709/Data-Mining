# Wentao Zhou
# DSCI 553 - data mining, Spring 2021, HW3, task1
# this projeect implements LSH algorithm.

from pyspark import SparkContext
import sys
import random
import itertools
import time
start = time.time()
sc = SparkContext('local[*]', 'task1')
sc.setLogLevel("ERROR")

# create list of hash functions.
# for each hash function h(x) = (ax + b) % numofbin
# so return a list of a and b.
def hash_function(num_hash):
    a = []
    for i in range(0, num_hash):
        rand = random.randint(0, num_hash * 10)
        while rand in a: # to avoid duplicate
            rand = random.randint(0, num_hash * 10)
        a.append(rand)
    b = []
    for i in range(0, num_hash):
        rand = random.randint(0, num_hash * 10)
        while rand in b:
            rand = random.randint(0, num_hash * 10)
        b.append(rand)
    return [a,b]

def hashf(u_index, hash_function_list, j, num_of_user):
    return (u_index * hash_function_list[0][j] + hash_function_list[1][j]) % num_of_user

# return a signature matrix (businees, [hash values.....])
# para: busi_setofuser:(business, {set of users ..... })
def sigmatrixandsplit(busi_setofuser, num_hash, hash_function_list, num_of_user, row):
    listofuser = list(busi_setofuser[1])
    sigmatrix = []
    # follow the steps from the slides
    for i in range(num_hash):
        sigmatrix.append(float('inf'))# initial the column of the sig matrix
    for i in range(len(listofuser)): # for each column c
        for j in range(num_hash): # each hash function
            u_index = user_dic[listofuser[i]]
            sigmatrix[j] = min(sigmatrix[j], hashf(u_index, hash_function_list, j, num_of_user)) # update values
    res = []
    # split matrix
    for i in range(0, num_hash, row):
        res.append((tuple([i] + sigmatrix[i: i+row]), busi_setofuser[0]))
    return res

# determine if the candidate pairs has similarity >= 0.5
# return pairs with similarity (pair, similiarity)
def judgecandidate(candidate_list,busi_dic):
    res = []
    for candidate_pair in candidate_list:
        totalset = busi_dic[candidate_pair[0]].union(busi_dic[candidate_pair[1]]) # union
        interset = busi_dic[candidate_pair[0]].intersection(busi_dic[candidate_pair[1]]) # intersection
        similarity = len(interset) / len(totalset)
        res.append((tuple(sorted(candidate_pair)), similarity))
    return res

num_hash = 50 # num of hash function
input_file_path = sys.argv[1]
output_file_path = sys.argv[2]
first_line = sc.textFile(input_file_path).first()
rdd = sc.textFile(input_file_path).filter(lambda row: row != first_line)\
    .map(lambda row: (str(row.split(',')[1]), str(row.split(',')[0])))
user_distinct = rdd.map(lambda x: x[1]).distinct().zipWithIndex()
user_dic = (dict(user_distinct.collect())) # key is user and value is its index
busi_user = rdd.groupByKey().mapValues(set)
busi_dic = dict(busi_user.collect()) # key is business, value is list of users
num_of_user = len(user_distinct.collect())
bands = 25
rows = num_hash // bands
hash_function_list = hash_function(num_hash)
signaturematrix = busi_user.map(lambda x: sigmatrixandsplit(x, num_hash, hash_function_list, num_of_user, rows))\
    .flatMap(lambda x: x).groupByKey().map(lambda x: (x[0], list(x[1]))).filter(lambda x: len(x[1]) > 1).map(lambda x: x[1])

# all business map to the same bucket, find the combinations of them.
candidate_list = signaturematrix.map(lambda x: itertools.combinations(x, 2))

true_similar = candidate_list.map(lambda x: judgecandidate(x, busi_dic)).flatMap(lambda x: x)\
    .filter(lambda x: x[1] >= 0.5).distinct().collect() # final results
true_similar.sort()
with open(output_file_path, 'w') as output:
    output.write("business_id_1, business_id_2, similarity\n")
    for pair in true_similar:
        output.write(str(pair[0][0]) + "," + str(pair[0][1]) + "," + str(pair[1]) + "\n")
print("Duration: " + str(time.time()-start))
