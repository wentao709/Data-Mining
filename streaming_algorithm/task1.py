# Wentao Zhou
# DSCI 553, HW5, Task1, Spring 2021

from blackbox import BlackBox
import sys
import random
import binascii

stream_size = int(sys.argv[2])
m = 69997
bx = BlackBox()
num_hash = 50
num_of_asks = int(sys.argv[3])
input_file_path = sys.argv[1]
output_file_path = sys.argv[4]

def myhashs(s):
    result = []
    for i in range(num_hash):
        s_num = int(binascii.hexlify(s.encode('utf8')),16)
        hash_value = ((hash_function_list[0][i] * s_num + hash_function_list[1][i]) % 70001) % 69997
        result.append(hash_value)
    return result

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
    return (a,b)


def bloom_filter(stream_users, m, count, previousUserIdSet):
    false_time = 0.0
    true = 0.0
    for user in stream_users:
        if user not in previousUserIdSet:
            flag = False
            hash_value_list = myhashs(user)
            for hash_value in hash_value_list:
                if filter_bit_array[hash_value] == 0:
                    filter_bit_array[hash_value] = 1
                    flag = True # it's not in the list
            if not flag:
                false_time += 1.0
            else:
                true += 1.0

    false_positive_rate = float(false_time) / (float(true) + float(false_time))
    with open(output_file_path, 'a') as output:
        output.write(str(count) + "," + str(false_positive_rate) + "\n")

with open(output_file_path, 'w') as output:
    output.write("Time,FPR\n")
hash_function_list = hash_function(num_hash)
previous = set()
filter_bit_array = [0] * m
for i in range(num_of_asks):
    stream_users = bx.ask(input_file_path, stream_size)
    bloom_filter(stream_users, m, i, previous)
    previous = set(stream_users)
