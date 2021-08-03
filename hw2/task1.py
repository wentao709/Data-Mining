# Wentao Zhou
# DSCI 553 - hw2, task1, spring 2021
# find frequent items using SON algorithm
from pyspark import SparkContext
import itertools
import sys
import time
start = time.time()
sc = SparkContext('local[*]', 'task1')
case = sys.argv[1]
s = sys.argv[2]
input_file_path = sys.argv[3]
output_file_path = sys.argv[4]
s = int(s)
combi_size = 2

# find all the combinations from a list
def combination(row, n):
    l = row
    return [v for v in itertools.combinations(l, n)]


# sort the list and return it as a tuple
def sort(row):
    return tuple(sorted(row))

baskets = sc.textFile(input_file_path).filter(lambda row: row != "user_id,business_id") # eliminate the head
numpartition = baskets.getNumPartitions()
if case == "1":
    baskets = baskets.map(lambda row: (str(row.split(',')[0]), str(row.split(',')[1])))\
        .groupByKey().mapValues(set).map(lambda x: x[1])
elif case == "2":
    baskets = baskets.map(lambda row: (str(row.split(',')[1]), str(row.split(',')[0])))\
        .groupByKey().mapValues(set).map(lambda x: x[1])


# I use APrioir algorithm to find candidates and frequent items.
# Param:
# partition: each partition is a subset of the whole input dataset.
# numpartition: the number of subset I have
def APriori(partition, numpartition):
    partition = tuple(partition)
    dic = {}
    candidate = []
    p = 1 / numpartition # use it to divide the threshold.
    # this loop find all the single candidate items
    for tup in partition:
        for element in tup:
            if element not in dic:
                dic[element] = 1
            else:
                dic[element] += 1
            if dic[element] == int(s // numpartition):  # only store the items whose number is >= threshold.
                sets = set()
                sets.add(element) # I store the integer in a set as later I need to combine the single item into tuple(a,b)
                if sets not in candidate:
                    candidate.append(sets)
    length = len(candidate)
    combi = 2 # combination number
    all_candidate = [] # store the output, which is all the candidates
    # this while loop finds all the combinations of candidates. I store each combination in a set
    # so like (a,b) (a,b,c)
    # the loops continue until
    while length > 0:
        all_candidate.append(candidate)  # store the final result
        all_possible = combination(candidate, combi_size)
        # above line is the key step, the candidate is the list of candidates(set),
        # so at first iteration, candidate is like [{100}, {200}, {150}, {120}], the output of combination is [{100, 150}, {100, 200}...]
        # at second iteration, candidate is like [{100,200}, {150, 200}, [150, 200]], the output is [{100, 200, 150}...]
        # I the combination size get incremented after each loop
        candidate = [] # store all candidates for a single iteration
        check_exist = set() # the combinations are likely to generate same results so the set can avoid duplicate
        for item in all_possible:
            iset = set()
            for it in item[0]:
                iset.add(it)
            for it in item[1]:
                iset.add(it)
            item = iset
            if (combi == len(item)): # key, for example at second iteration combi == 3, I only want set of size 3,
                # but the above eexample might generate [{100}, {200}, {150}, {120}], but I don't want it at second iteration
                # so I get rid of it.
                if tuple(sorted(tuple(item))) not in check_exist: # avoid duplicate
                    check_exist.add(tuple(sorted(tuple(item))))
                    for tup in partition:
                        if item.issubset(tup):
                            itemtuple = tuple(sorted(tuple(item)))
                            if itemtuple in dic:
                                dic[itemtuple] += 1
                            else:
                                dic[itemtuple] = 1
                            if (dic[itemtuple] == int(s // numpartition)):
                                candidate.append(item) # find candidates
                                break # each item go through all the baskets, once the number exceed threshold, stop and go for next item.
        length = len(candidate)
        combi += 1 # next loop increment of size of sets(candidates)
    return all_candidate

# phase 2 of SON algorithm
def phase2(basket, candidate1):
    basket = tuple(basket)
    res = [] # store all the candidates
    for element in candidate1:
        set_ele = set(element)
        for tuples in basket:  # each element go through all baskets, store each element with number 1. so like (candidate, 1)
            if set_ele.issubset(tuples):
                res.append((tuple(sorted(tuple(set_ele))), 1))
    return res


# write output to output file path
def write_output(candidate1):
    length = len(candidate1)
    tuple_length = len(candidate1[0])
    tuple_list = [] # collect all candidates with same size.
    for i in range(length):
        if tuple_length == len(candidate1[i]): # collect all candidates with same size.
            tuple_list.append(candidate1[i])
        else: # done collect all candidates with same size, now write output.
            tuple_list.sort() # sort the list of tuples.
            if tuple_length == 1:  # candidates with single element, it's like this ('101',), so it's in a different than ('101', '102')
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
    # I didn't write the output for the last iteration, so
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
candidate = baskets.mapPartitions(lambda partition: APriori(partition, numpartition))\
    .flatMap(lambda x: x).map(lambda x: tuple(sorted(tuple(x)))).distinct().collect()
candidate1 = sorted(list((candidate)), key=lambda x: (len(x)))
write_output(candidate1) # output of phase 1

# phase 2
candidate2 = baskets.mapPartitions(lambda partition: phase2(partition, candidate1)) \
    .reduceByKey(lambda a, b: a + b).filter(lambda x: x[1] >= s).map(lambda x: x[0]).collect()
candidate2 = sorted(list((candidate2)), key=lambda x: (len(x)))
with open(output_file_path, 'a') as output:
    output.write("Frequent Itemsets:\n")
write_output(candidate2)
end = time.time()
print("Duration: " + str(end - start))