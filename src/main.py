import asyncio
import random
import time
from datetime import datetime

from tools.manipulate_path_dir import create_path
from utils.args import ArgsException, parse_args, validate_and_rename_args
from celery_models.prefork import cpu_intensive_task, memory_wave_task
from monitoring.process_monitor import ProcessMonitor
from monitoring.queue_monitor import QueueMonitor


max_process = {
    '02': {'idx_pause_task': 2,
        'max_count_task': 381},
    '04': {'idx_pause_task': 4,
        'max_count_task': 761},
    '08': {'idx_pause_task': 8,
        'max_count_task': 1201}, # 1521
}


async def start_monitor_queue(throughput_monitor) -> None:
    """
    Мониторинг очереди RabbitMQ
    """
    try:
        while True:
            await throughput_monitor.update_stats()
            await asyncio.sleep(5.0)
    except asyncio.CancelledError:
        await throughput_monitor.purge_queue()
        await throughput_monitor.update_stats()
        await throughput_monitor.write_to_file()
        raise


async def start_monitor_process(monitor) -> None:
    """
    Мониторинг celery-процессов
    """
    try:
        while True:
            # На всякий случай выносим в отдельный поток,
            # так как неизвестно как поведет себя чтение проца
            # при 8, 12 и тд рабочих процессах.
            # /proc обычно буферизируется ядром, и асинхронность врд ли даст выигрыша
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
    - монотонная, где задачи не копятся в статусе Ready в брокере, так как частота публикации подогнана к латенси;
    - накопительная, где задачи могут ожидать освобождения процесса.
    Темп для формирования очереди задается временем паузы.
    Конечно можно обойтись без паузы и просто навалить задач в очередь
    и ждать пока воркер все их разгребет. +\- это будет то же кол-во задач.
    Но а) я не хочу заморачиваться по поводу высчитывания интервала от завершения основного скрипта
    до приблизительного окончания обработки; б) умозрительно я считаю, что такой подход не повлияет сильно на метрики.

    Для худо-бедно имитации разного типа задач меняем место паузы для cpu_intensive_task до или после основного скрипта,
    а каждую третью задачу ставим memory_wave_task.

    По окончанию цикла выдерживаем паузу в 4 секунды, чтобы задачи, скопившиеся в очереди точно были исполнены.
    """
    # Чтобы сбор статистики по очереди не пропустил первые задачи
    await asyncio.sleep(0.5)

    paused_task = max_process[max_proc]['idx_pause_task']

    print(f'Start at {datetime.now()}')
    time_sleep = 1.7 if 'monotonic' in file_path else 1.6
    # При тестовом прогоне было замечено, что пропускная способность реально выросла в два раза,
    # но очередь быстро росла, поэтому решил увеличить паузу, чтобы не накапливать очередь
    # Интересное наблюдение при concurrency=8 монотонной очереди:
    # - при паузе больше базовой в два раза -- даже очередь неподтвержденных(Unacked) задач пуста (throughput = 2.32; latency = 1.82);
    # - при стандартной "монотонной" паузе -- очередь быстро растет (throughput = 3.97; latency = 1.97).
    # - при паузе Х1,5 -- очередь становится равномерной (throughput = 3.09, latency = 1.82).
    #   И при пайзе 1.25, и 1.4 становится равномерной. Не угадаешь. Будем угадывать.
    #  А на выходе чем медленнее поступают задачи, тем меньше пропускная способность. Ло! Логика.
    # Главное, чтобы очередь не переполнялась больше, чем рабочих процессов, и не пустовала
    if max_proc == '08' and queue_type == 'm':
        multipliers = [1.2, 1.25, 1.3, 1.4, 1.5]
        multiplier = random.choice(multipliers)
        print(f"Random choice multiplier {multiplier}")
        time_sleep *= multiplier

    max_duration = 300
    start_time = time.time()
    idx_task = 0
    while (enqueue_time := time.time()) - start_time < max_duration:
        idx_task += 1
        if idx_task % 3 == 0:
            memory_wave_task.delay(idx_task, file_path, enqueue_time)
        else:
            cpu_intensive_task.delay(idx_task, file_path, enqueue_time)

        if idx_task % paused_task == 0:
            await asyncio.sleep(time_sleep)

    print(f'End at {datetime.now()}, sent {idx_task} tasks')
    await asyncio.sleep(time_sleep)


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
