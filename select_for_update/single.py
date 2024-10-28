import time
import datetime
import functools
import multiprocessing


def single(max_processing_time=datetime.timedelta(minutes=2)):
    lock = multiprocessing.Lock()  # Создаём блокировку

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Пробуем захватить блокировку с таймаутом
            acquired = lock.acquire(timeout=max_processing_time.total_seconds())
            if not acquired:
                print(f"Не удалось захватить блокировку в течение {max_processing_time}.")
                return

            try:
                # Запускаем декорируемую функцию
                return func(*args, **kwargs)
            finally:
                # Освобождаем блокировку
                lock.release()

        return wrapper

    return decorator


# Пример использования
@single(max_processing_time=datetime.timedelta(minutes=2))
def process_transaction():
    print("Начинается обработка транзакции")
    time.sleep(2)  # Симулируем работу
    print("Транзакция обработана")


if __name__ == "__main__":
    from multiprocessing import Process

    processes = [Process(target=process_transaction) for _ in range(5)]

    for p in processes:
        p.start()

    for p in processes:
        p.join()
