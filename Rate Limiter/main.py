import time
from redis_client import RedisClient


class TokenBucketRateLimiter():
    def __init__(self, capacity: int, fill_rate: int):
        # Consider all user have the same capacity, and fill_rate
        self.capacity = capacity
        self.fill_rate = fill_rate # Unit: token per sec

        # Initilaize a redis client for storing last_visit_time
        self.redis = RedisClient()

    
    def allow_request(self, user: str) -> bool:
        now = int(time.time())

        last_visit_time = self.redis.get(f'{user}:last_visit_time')
        tokens = self.redis.get(f'{user}:tokens')

        tokens = min(self.capacity, int(self.fill_rate * (now - last_visit_time)) + tokens)

        self.redis.set(f'{user}:last_visit_time', now)

        if tokens > 0:
            self.redis.set(f'{user}:tokens', tokens - 1)
            return True
        
        return False

rate_limiter = TokenBucketRateLimiter(capacity=20, fill_rate=5)

for i in range(20):
    req = rate_limiter.allow_request("james")

    if req:
        print("Allow request")
    else:
        print("Request Denied")
    

            

