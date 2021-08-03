# Wentao Zhou
# DSCI 553, HW5, Task2, Spring 2021

from blackbox import BlackBox
import sys
import random
import binascii

input_file_path = sys.argv[1]
stream_size = int(sys.argv[2])
num_of_asks = int(sys.argv[3])
output_file_path = sys.argv[4]
bx = BlackBox()
num_hash = 50

def myhashs(s):
    result = []
    for i in range(num_hash):
        s_num = int(binascii.hexlify(s.encode('utf8')),16)
        hash_value = ((hash_function_list[0][i] * s_num + hash_function_list[1][i]) % 701) % 700
        result.append(hash_value)
    return result

def hash_function(num_hash, numofuser):
    ## Generate random values for a and b.
    a = []
    for i in range(0, num_hash):
        rand = random.randint(0, numofuser)
        while rand in a:  # to avoid duplicate
            rand = random.randint(0, numofuser)
        a.append(rand)
    b = []
    for i in range(0, num_hash):
        rand = random.randint(0, numofuser)
        while rand in b:
            rand = random.randint(0, numofuser)
        b.append(rand)
    ## Return the hashed value.
    return (a,b)

hash_function_list = hash_function(num_hash, 700)


def flajolet(stream_users, hash_function_list, index):
    length = 0
    average = []
    for j in range(0, num_hash): # partition hash function into groups
        maxcount = 0
        globalUserIdSet = set()
        for user in stream_users:
            user_num = int(binascii.hexlify(user.encode('utf8')), 16)
            count = 0
            a = hash_function_list[0][j]
            b = hash_function_list[1][j]
            hash_value = ((a * user_num + b) % 701) % 700
            binary_hash_value = bin(hash_value)[2:]
            for i in range(len(binary_hash_value)-1, -1, -1):
                if binary_hash_value[i] == '0':
                    count += 1
                else:
                    break
            if (maxcount < count):
                maxcount = count
            globalUserIdSet.add(user)
        length = len(globalUserIdSet)
        average.append(2 ** maxcount)
    res = []
    # window size hash values into partitions

    for i in range(0, 10):
        sums = sum(average[i*5:i*5+5])/len(average[i*5:i*5+5])
        res.append(sums)
    res.sort()
    median = (res[4]+res[5])/2
    with open(output_file_path, 'a') as output:
       output.write(str(index) + "," + str(length) + "," + str(int(median)) + "\n")

with open(output_file_path, 'w') as output:
    output.write("Time,Ground Truth,Estimation\n")
for i in range(num_of_asks):
    stream_users = bx.ask(input_file_path, stream_size)
    flajolet(stream_users, hash_function_list, i)