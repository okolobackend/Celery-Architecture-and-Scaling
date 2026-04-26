import asyncio
import time
from datetime import datetime

from tools.manipulate_path_dir import create_path
from utils.args import ArgsException, parse_args, validate_and_rename_args
from celery_models.prefork import cpu_intensive_task, memory_wave_task
from monitoring.process_monitor import ProcessMonitor
from monitoring.queue_monitor import QueueMonitor


async def start_monitor_queue(throughput_monitor) -> None:
    """
    Мониторинг очереди RabbitMQ.
    Важна последовательность мониторинга:
    1. Первый срез должен быть получен ДО начала цикла постановки задач.
        Этому должна способствовать пауза await asyncio.sleep(time_sleep)
    2. За время работы скрипта мы отправим примерно 60 запросов(300 сек(общая длительность)/5 сек(здесь)
    3. После отправки сигнала о завершении работы:
        - Сбросить очередь
        - Подождать 2 секунды ДО завершения задач, которые были взяты в работы после конца скрипта
            (такое поведение встретится обязательно при накопительной очереди)
    4. Зафиксировать статистику при пустой очереди
    5. Записать в файл
    """
    try:
        while True:
            await throughput_monitor.update_stats()
            await asyncio.sleep(5.0)
    except asyncio.CancelledError:
        await throughput_monitor.purge_queue()
        # Асинхронный слип не работает! После него не пишется лог-файл, так как
        # await asyncio.sleep() внутри except CancelledError вызывает новый CancelledError,
        # потому что задача уже отменена и дальнейший код не исполняется
        time.sleep(2.0)
        await throughput_monitor.update_stats()
        await throughput_monitor.write_to_file()
        raise


async def start_monitor_process(monitor) -> None:
    """
    Мониторинг celery-процессов
    На всякий случай выносим в отдельный поток,
    так как неизвестно как поведет себя чтение проца
    при 8, 12 и тд рабочих процессах.
    /proc обычно буферизируется ядром, и асинхронность врд ли даст выигрыша.
    УТОЧНИТЬ В НОРМАЛЬНЫХ ИСТОЧНИКАХ, А НЕ ДИПСИК
    """
    try:
        while True:
            await asyncio.to_thread(monitor.update_stats)
            await asyncio.sleep(1.0)
    except asyncio.CancelledError:
        await asyncio.to_thread(monitor.update_stats)
        await monitor.format_stats()
        raise


async def main_script(file_path: str, max_proc: str, queue_type: str) -> None:
    """
    Запуск скрипта тестовых прогонов.

    С помощью max_proc(максимальное кол-во рабочих процессов воркера) определяем
    кол-во задач и место паузы генерации этих задач.

    Есть два типа очередей для тестов:
    - по расписанию, имитация Celery Beat;
    - накопительная, время поступления новой пачки чуть меньше среднего runtime.
    Темп для формирования очереди задается временем паузы.
    Конечно можно обойтись без паузы и просто навалить задач в очередь
    и ждать пока воркер все их разгребет. +/- это будет то же кол-во задач.
    Но а) я не хочу заморачиваться по поводу высчитывания интервала от завершения основного скрипта
    до приблизительного окончания обработки; б) умозрительно я считаю, что такой подход не сильно повлияет на метрики.

    Для худо-бедно имитации разного типа задач меняем место паузы для cpu_intensive_task до или после основного скрипта,
    а каждую третью задачу ставим memory_wave_task.
    """
    # Чтобы сбор статистики по очереди не пропустил первые задачи
    await asyncio.sleep(0.5)

    print(f'Start at {datetime.now()}')
    time_sleep = 10 if 'schedule' in file_path else 1.6

    max_duration = 300
    start_time = time.time()
    idx_task = 0
    paused_task = int(max_proc)

    while (enqueue_time := time.time()) - start_time < max_duration:
        idx_task += 1
        if idx_task % 3 == 0:
            memory_wave_task.delay(idx_task, file_path, enqueue_time)
        else:
            cpu_intensive_task.delay(idx_task, file_path, enqueue_time)

        if idx_task % paused_task == 0:
            await asyncio.sleep(time_sleep)

    print(f'End at {datetime.now()}, sent {idx_task} tasks')


async def main() -> None:
    args = parse_args()

    try:
        max_proc_str, worker_dir, queue_dir, order_dir = validate_and_rename_args(args)
        file_path = create_path(max_proc_str, worker_dir, queue_dir, order_dir)
    except (ArgsException, PermissionError, OSError) as e:
        print(e)
        return

    monitor = ProcessMonitor(file_path)
    second_monitor = QueueMonitor(output_dir=file_path)

    monitor_process = asyncio.create_task(start_monitor_process(monitor))
    monitor_queue = asyncio.create_task(start_monitor_queue(second_monitor))

    try:
        await main_script(file_path, max_proc_str, args.queue_type)
    finally:
        monitor_process.cancel()
        monitor_queue.cancel()
        try:
            await monitor_process
            await monitor_queue
        except asyncio.CancelledError:
            pass


if __name__ == '__main__':
    asyncio.run(main())
