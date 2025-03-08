import redis

class RedisClient:
    def __init__(self):
        # Run redis using docker
        # docker run -p 6379:6379 --name redis -d redis
        self.client = redis.Redis(host='localhost', port=6379, db=0)

    def set(self, key, value) -> None:
        self.client.set(key, value)
    
    def get(self, key) -> any:
        res = self.client.get(key)

        return int(res.decode('utf-8')) if res != None else 0
    

