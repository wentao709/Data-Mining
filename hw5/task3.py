# Wentao Zhou
# DSCI 553, HW5, Task3, Spring 2021

from blackbox import BlackBox
import sys
import random

input_file_path = sys.argv[1]
stream_size = int(sys.argv[2])
num_of_asks = int(sys.argv[3])
output_file_path = sys.argv[4]
rand = random.seed(553)
bx = BlackBox()
global fixed_sample

def reservior(stream_users, index, fixed_sample):
    for i in range(len(stream_users)):
        prob = random.random()
        if prob < float(100 / (100 * index + i + 1)):
            random_index = random.randint(0,99)
            fixed_sample[random_index] = stream_users[i]
    with open(output_file_path, 'a') as output:
        output.write(str((index+1) * 100) + "," + str(fixed_sample[0]) + "," + str(fixed_sample[20]) + "," +
                     str(fixed_sample[40]) + "," + str(fixed_sample[60]) + "," + str(fixed_sample[80]) + "\n")

with open(output_file_path, 'w') as output:
    output.write("seqnum,0_id,20_id,40_id,60_id,80_id\n")
for i in range(num_of_asks):
    stream_users = bx.ask(input_file_path, stream_size)
    if i == 0:
        fixed_sample = stream_users
        with open(output_file_path, 'a') as output:
            output.write(str((i + 1) * 100) + "," + str(fixed_sample[0]) + "," + str(fixed_sample[20]) + "," +
                         str(fixed_sample[40]) + "," + str(fixed_sample[60]) + "," + str(fixed_sample[80]) + "\n")
    else:
        reservior(stream_users, i, fixed_sample)