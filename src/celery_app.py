import time

from celery import Celery, signals


def make_celery():
    celery = Celery(
        broker=f'amqp://celery_demo:celery_demo@localhost:5672/',
    )

    celery.conf.update(
        task_serializer='json',
        accept_content=['json'],
        result_serializer='json',
        timezone='Europe/Moscow',
        enable_utc=True,
        # Лимит предварительной выборки —
        # это ограничение количества задач (сообщений),
        # которые рабочий процесс может зарезервировать для себя.
        # Если он равен нулю, рабочий процесс продолжит потреблять сообщения,
        # не учитывая, что могут быть другие доступные рабочие узлы,
        # которые смогут обработать их раньше,
        # или что сообщения могут даже не помещаться в память.
        worker_prefetch_multiplier=1,
        # Максимальное количество задач, могущее обработать один "потомок".
        # При достижении лимита создается новый процесс.
        # Проверял на concurrency.
        # Для autoscale, думаю, не особо актуально
        # из-за собственных механизмов порождения и остановы процессов
        # worker_max_tasks_per_child=100,
        # Максимальное потребление памяти ДО ЗАМЕНЫ(!!!) одним рабочим процессом внутри воркера.
        # То есть у меня вышло 200 лог-файлов(по одному на задачу) при текущем ограничении.
        # Внутри самой задачи используется примерно столько же памяти
        # worker_max_memory_per_child=60000,
        task_acks_late=True,
        broker_pool_limit=None,
        imports=('celery_models.prefork',)
    )

    return celery


# Создаем экземпляр Celery
celery = make_celery()


if __name__ == "__main__":
    import sys
    print("Запуск Celery worker с параметрами по умолчанию (concurrency=2)")
    print("Для настройки используйте аргументы командной строки, например:")
    print("  celery -A celery_app worker --autoscale=4,0 --loglevel=info")
    # Можно передать аргументы напрямую, но проще показать пример
    sys.argv = ["-A", "celery_app", "worker", "--loglevel=info", "--concurrency=2"]
    celery.worker_main()
