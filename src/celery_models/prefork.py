import gc
import time

from celery import shared_task

from monitoring.middleware import os_stats


@shared_task(name='cpu_intensive', bind=True)
@os_stats
def cpu_intensive_task(self, idx_task: int, file_path: str, enqueue_time: float) -> int:
    """
    :param self: для логирования
    :param idx_task: для логирования и четности для определения места паузы
    :param file_path: место записи логов
    :param enqueue_time: время отправки в очередь, для лога
    """
    result = 0
    time_sleep = 1.0
    even = True if idx_task % 2 == 0 else False

    if even:
        time.sleep(time_sleep)

    for i in range(10**7):
        result += i**2

    if not even:
        time.sleep(time_sleep)

    return result


@shared_task(name='memory_intensive', bind=True)
@os_stats
def memory_wave_task(self, idx_task: int, file_path: str, enqueue_time: float, peak_size: int = 100000, step: int = 10000, delay: float = 0.1):
    """
    Постепенно накапливает данные, затем очищает память.

    По умолчанию длительность около 1.6 сек.

    Celery не управляет памятью автоматически, поэтому при большом(> 10_000_000) `peak_size`
    и большом количестве одновременных задач воркер может упасть.

    Режим берсеркера: data = ['a' * 1000 for _ in range(10 ** multiplier)]
        - multiplier=4 → 10 000 строк ≈ 10 МБ (плюс накладные расходы на список и объекты) — безопасно.
        - multiplier=5 → 100 000 строк ≈ 100 МБ — уже ощутимо, но может пройти.
        - multiplier=6 → 1 млн строк ≈ 1 ГБ — риск для типичного воркера с памятью 2–4 ГБ.
        - multiplier=7 → 10 млн строк ≈ 10 ГБ — почти гарантированный MemoryError.

    :param self: для логирования
    :param idx_task: для логирования
    :param file_path: место записи логов
    :param enqueue_time: время отправки в очередь, для лога
    :param peak_size: максимальное количество элементов в списке
    :param step: размер шага накопления
    :param delay: задержка между шагами (сек)
    """
    data = []
    for i in range(0, peak_size, step):
        chunk = [f"item_{j}" * 100 for j in range(step)]
        data.extend(chunk)
        time.sleep(delay)

    time.sleep(0.6)

    # Освобождаем память
    # gc.set_threshold(700, 10, 10)  # Default: (700, 10, 10)
    # Lower thresholds trigger collection more frequently, trading CPU cycles for memory efficiency.

    data = None
    gc.collect()
    return "Memory wave: released"
