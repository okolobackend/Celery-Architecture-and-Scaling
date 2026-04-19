import time
from datetime import datetime

from celery_models.prefork import cpu_intensive_task


def main() -> None:

    print(f'Start at {datetime.now()}')
    for idx_task in range(1, 601):
        cpu_intensive_task.delay(idx_task, 7)
        if idx_task % 16 == 0:
            print(f'Pause after idx_task: {idx_task}')
            # time.sleep(9)

        # if idx_task % 40 == 0:
        #     print(f'Pause (BIG!) after idx_task: {idx_task}')
        #     time.sleep(20)

    print(f'End at {datetime.now()}')


if __name__ == '__main__':
    main()
