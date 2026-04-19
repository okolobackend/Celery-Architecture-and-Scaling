import time
from datetime import datetime

from celery_models.prefork import cpu_intensive_task


def main() -> None:

    print(f'Start at {datetime.now()}')

    for idx_task in range(1, 121):
        # Пусть каждая третья задача будет большой
        multiplier = 8 if idx_task % 3 == 0 else 7
        cpu_intensive_task.delay(idx_task, multiplier)

        if idx_task % 4 == 0:
            print(f'Pause after idx_task: {idx_task}')
            time.sleep(10)

    print(f'End at {datetime.now()}')


if __name__ == '__main__':
    main()
