import random
import time
import redis


class RateLimitExceed(Exception):
    pass


class RateLimiter:
    def __init__(self, redis_client: redis.Redis, key: str, limit: int = 5, period: int = 3):
        self.redis_client = redis_client  # Redis клиент
        self.key = key  # Ключ для хранения меток времени
        self.limit = limit  # Максимальное количество запросов
        self.period = period  # Период времени в секундах

    def test(self) -> bool:
        current_time = time.time()
        # Добавляем текущую метку времени в Redis
        self.redis_client.zadd(self.key, {current_time: current_time})
        # Удаляем метки, которые старше периода
        self.redis_client.zremrangebyscore(self.key, 0, current_time - self.period)

        # Получаем количество меток времени в текущем периоде
        count = self.redis_client.zcard(self.key)

        if count <= self.limit:
            return True
        else:
            return False

    def cleanup(self):
        # Метод для очистки старых ключей, если необходимо
        self.redis_client.delete(self.key)


def make_api_request(rate_limiter: RateLimiter):
    if not rate_limiter.test():
        raise RateLimitExceed
    else:
        # тут должна быть логика работы API
        print("API request made.")


if __name__ == '__main__':
    # Настройка Redis
    redis_client = redis.Redis(host='localhost', port=6379, db=0)
    rate_limiter = RateLimiter(redis_client=redis_client, key='api_requests')

    for _ in range(50):
        time.sleep(random.randint(1, 2))

        try:
            make_api_request(rate_limiter)
        except RateLimitExceed:
            print("Rate limit exceed!")
        else:
            print("All good")
