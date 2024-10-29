import psycopg2
import time


def connection():
    return psycopg2.connect(database='select_for_update',
                            user='postgres',
                            password='5728821q',
                            host='localhost',
                            port='5432')


def task_queue():
    conn = connection()
    with conn.cursor() as cur:
        cur.execute("""
        DROP TABLE IF EXISTS task_queue
        """);
        conn.commit()
        
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS task_queue(
            id SERIAL PRIMARY KEY,
            task_name VARCHAR(255) NOT NULL,
            status VARCHAR(255) NOT NULL,
            worker_id INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            """);
        conn.commit()


def fetch_task(worker_id):
    conn = connection()
    with conn.cursor() as cur:
        cur.execute("""
        SELECT id, task_name
        FROM task_queue
        WHERE status = 'pending'
        FOR UPDATE SKIP LOCKED
        LIMIT 1
        """);
        task = cur.fetchone()
        if task is not None:
            print('Актуальных задач нет')
            return None
        else:
            cur.execute("""
            UPDATE task_queue
            SET status = 'in_progress', worker_id = %s
            WHERE id = %s
            """, (worker_id, task[0]));
            conn.commit()
            task_id = task[0]
            task_name = task[1]
            return task_id, task_name


def complete_task(task_id):
    conn = connection()
    with conn.cursor() as cur:
        cur.execute("""
        UPDATE task_queue
        SET status = 'completed', worker_id = NULL
        WHERE id = %s
        """, (task_id,))
        conn.commit()


def processing(task_id):
    try:
        time.sleep(5)  # имитация бурной деятельности
        print(f'Задача {task_id} обработана')
        complete_task(task_id)
    except Exception as e:
        print(f'Ошибка при обработке задачи {task_id}: {e}')


def worker_process(worker_id):
    while True:
        task = fetch_task(worker_id)
        if task:
            task_id, task_name = task
            print(f'Работник {worker_id} взял задачу {task_id}: {task_name}')
            processing(task_id)
        else:
            print('Ожидание новых задач...')
            time.sleep(5)  # Ожидание перед следующим запросом


if __name__ == "__main__":
    task_queue()
    worker_id = 1
    print(f"Рабочий процесс с ID {worker_id} запущен.")
    worker_process(worker_id)
