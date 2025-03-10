from collections import defaultdict
from dataclasses import dataclass
from hashing import ConsistentHashing


# 1. Test data distribution
consistent_hash = ConsistentHashing(1024, 20)
data_num = 200
servers = ["server" + str(i) for i in range(4)]
data = ["data" + str(i) for i in range(data_num)]

# Add servers into hash space
for i in range(len(servers)):
    consistent_hash.add_server(servers[i])

# Add data into hash space
for i in range(len(data)):
    consistent_hash.add_key(data[i])

# Get which data go to which server
server_cnt = defaultdict(int)
server_used = [None] * data_num

for i in range(len(data)):
    server = consistent_hash.find_server(data[i])

    physical_server = server.split('_')[0]
    server_cnt[physical_server] += 1
    server_used[i] = physical_server

    print(f"{data[i]} uses {server}")

# Get the percentage of data distribution on each server
for i in range(len(servers)):
    print(f"{servers[i]}: {server_cnt[servers[i]] / len(data)}")


# 2. Test the numbers of data that need to be re-distributed after server removed

# (1) Remove a server
removed_server_idx = 3
consistent_hash.remove_server(servers[removed_server_idx])

# (2) Get differences
server_cnt = defaultdict(int)
new_server_used = [None] * data_num

for i in range(len(data)):
    server = consistent_hash.find_server(data[i])

    physical_server = server.split('_')[0]
    server_cnt[physical_server] += 1
    new_server_used[i] = physical_server

    print(f"{data[i]} uses {server}")

# Get the new percentage of data distribution on each server
for i in range(len(servers)):
    if i != removed_server_idx:
        print(f"{servers[i]}: {server_cnt[servers[i]] / len(data)}")

diff_cnt = 0

for s1, s2 in zip(server_used, new_server_used):
    diff_cnt += (s1 != s2)

print(f"Number of key re-distributed: {diff_cnt}")

