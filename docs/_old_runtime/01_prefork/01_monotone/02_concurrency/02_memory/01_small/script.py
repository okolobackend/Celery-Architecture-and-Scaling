import time
from datetime import datetime

from celery_models.prefork import memory_wave_task


def main() -> None:

    print(f'Start at {datetime.now()}')

    for idx_task in range(1, 201):
        memory_wave_task.delay(idx_task)

        if idx_task % 4 == 0:
            print(f'Pause after idx_task: {idx_task}')
            time.sleep(6)

    print(f'End at {datetime.now()}')


if __name__ == '__main__':
    main()
