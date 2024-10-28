import redis
import json


class RedisQueue:
    def __init__(self, host='localhost', port=6379, db=0, queue_name='my_queue'):
        self.redis_client = redis.StrictRedis(host=host, port=port, db=db)
        self.queue_name = queue_name

    def publish(self, msg: dict):
        # Добавляем сообщение в очередь
        self.redis_client.rpush(self.queue_name, json.dumps(msg))

    def consume(self) -> dict:
        # Извлекаем сообщение из очереди, дожидаясь, если очередь пуста
        msg = self.redis_client.lpop(self.queue_name)
        if msg is None:
            return None
        return json.loads(msg)


if __name__ == '__main__':
    q = RedisQueue()

    q.publish({'a': 1})
    q.publish({'b': 2})
    q.publish({'c': 3})

    assert q.consume() == {'a': 1}
    assert q.consume() == {'b': 2}
    assert q.consume() == {'c': 3}
    assert q.consume() is None  # Проверяем, что больше сообщений нет
