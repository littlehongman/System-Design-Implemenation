import hashlib


class ConsistentHashing():
    def __init__(self, length, virtual_node_num = 10):
        self.hash_length = length
        self.hash_space = [None] * length
        # Store the position where the node or data is hashed to easily know where to remove later
        self.key_dict = {}
        self.data_cnt = 0

        self.virtual_node_num = virtual_node_num
    
    def hash_func(self, key):
        if not isinstance(key, bytes):
            key = str(key).encode('utf-8')
    
        # Calculate SHA-1 hash
        sha1_hash = hashlib.sha1(key).hexdigest()
        
        # Using the first 8 characters of the hex hash (32 bits)
        pos = int(sha1_hash[:8], 16) % self.hash_length

        while self.hash_space[pos] != None:
            pos = (pos + 1) % self.hash_length
        
        return pos

    def add_server(self, server_name: str):
        for i in range(self.virtual_node_num):
            self.add_key(server_name + '_v' + str(i))
    
    def remove_server(self, server_name: str):
        for i in range(self.virtual_node_num):
            self.remove_key(server_name + '_v' + str(i))

    def add_key(self, key):
        pos = self.hash_func(key)

        self.hash_space[pos] = key
        self.key_dict[key] = pos

        if 'data' in key:
            self.data_cnt += 1
    
    def remove_key(self, key):
        pos = self.key_dict[key]
        self.hash_space[pos] = None
    
    def get_pos(self, key):
        return self.key_dict[key]
    
    def find_server(self, data):
        pos = self.key_dict[data]

        while True:
            pos = (pos + 1) % self.hash_length
            node = self.hash_space[pos]

            # A very simple way to identify a server with its key name
            if node and 'server' in node:
                return node
    