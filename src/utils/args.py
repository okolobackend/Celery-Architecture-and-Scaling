import argparse
from typing import Sequence


type_worker = {
    'a': 'autoscale',
    'c': 'concurrency'
}

type_queue = {
    'v': 'cumulative',
    's': 'schedule'

}

order_name = {
    1: '01_first',
    2: '02_second',
    3: '03_third',
    4: '04_fourth',
    5: '05_fifth',
    6: '06_sixth',
    7: '07_seventh',
    8: '08_eight',
    9: '09_ninth',
    10: '10_tenth',
    11: '11_eleventh',
    12: '12_twelfth',
    13: '13_thirteenth',
    14: '14_fourteenth',
    15: '15_fifteenth',
    16: '16_sixteenth',
    17: '17_seventeenth',
    18: '18_eighteenth',
    19: '19_nineteenth',
    20: '20_twentieth',
}


class ArgsException(Exception):
    pass


def parse_args():
    parser = argparse.ArgumentParser(
        description="Скрипт для тестов Celery",
        epilog="Пример: python script.py a v 1 4"
    )
    parser.add_argument('worker_type', choices=['a', 'c'],
                        help="Тип воркера: 'a' - autoscale, 'c' - concurrency")
    parser.add_argument('max_proc', type=int, choices=[2, 4, 8],
                        help="Максимум процессов: 2, 4 или 8")
    parser.add_argument('queue_type', choices=['v', 's'],
                        help="Тип очереди: 'v' - cumulative, 's' - schedule")
    parser.add_argument('order', type=int, choices=range(1, 21),
                        help="Номер запуска от 1 до 20")

    return parser.parse_args()


def validate_and_rename_args(args) -> Sequence[str]:
    """
    Проверяем аргументы и переименовываем их в наименования директорий
    """
    if args.worker_type not in type_worker.keys():
        raise ArgsException("Нет такого режима для prefork!")
    if args.order not in order_name.keys():
        raise ArgsException("Нет такого порядкового номера!")
    if args.queue_type not in type_queue.keys():
        raise ArgsException("Нет такого режима создания задач!")

    return f"{args.max_proc:02d}", type_worker[args.worker_type], type_queue[args.queue_type], order_name[args.order]
