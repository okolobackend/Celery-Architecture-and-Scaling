#!/usr/bin/env python3
"""
Скрипт для сбора итоговой статистики тестов Celery.
Ожидается структура:

docs/
    02_processes/
        autoscale/
            cumulative/
                01_first/
                    queue_throughput.log
                    summary.log
                02_second/
                ...
            schedule/
                ...
        concurrency/
            cumulative/
            schedule/
    04_processes/
    ...

Использование:
    python collect_stats.py [--root PATH] [--output FILE]

По умолчанию root = ../../docs/02_runtime, output = summary_report.txt
"""

import argparse
import json
import math
import re
from pathlib import Path
from typing import Dict, List, Tuple, Optional

from tools.manipulate_path_dir import get_default_root
from utils.args import QUEUE_TYPE, WORKER_TYPE
from utils.collect_stats import t_critical


def parse_queue_throughput(file_path: Path) -> Tuple[float, int, float]:
    """
    Извлекает из queue_throughput.log суммарные значения throughput_tps,
    tasks_completed и duration_sec по всем активным периодам.
    Возвращает (throughput, tasks, duration).
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        print(f"  Предупреждение: не удалось прочитать {file_path}: {e}")
        return 0.0, 0, 0.0

    periods = data.get('active_periods', [])
    if not periods:
        return 0.0, 0, 0.0

    total_throughput = 0.0
    total_tasks = 0
    total_duration = 0.0
    for period in periods:
        total_throughput += period.get('throughput_tps', 0.0)
        total_tasks += period.get('tasks_completed', 0)
        total_duration += period.get('duration_sec', 0.0)
    return total_throughput, total_tasks, total_duration


def parse_summary(file_path: Path) -> Optional[Dict[str, float]]:
    """
    Парсит summary.log, извлекает данные только для рабочих процессов.
    Возвращает словарь:
        {
            'avg_latency': float,      # среднее latency по всем рабочим процессам
            'avg_tasks_per_worker': float,  # среднее число задач на процесс
            'avg_voluntary': float,    # среднее voluntary контекстных переключений
            'avg_nonvoluntary': float, # среднее nonvoluntary
            'worker_count': int        # количество рабочих процессов
        }
    Если рабочих процессов нет, возвращает None.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
    except OSError as e:
        print(f"  Предупреждение: не удалось прочитать {file_path}: {e}")
        return None

    # Регулярное выражение для поиска ключ: значение
    key_value_re = re.compile(r'^([A-Za-zА-Яа-я\s_]+):\s*([\d\.]+)')
    # Признак управляющего процесса
    is_controller = False
    current_data = {}
    workers_data = []  # список словарей с данными каждого рабочего процесса

    for line in lines:
        line = line.strip()
        if not line:
            continue

        # Начало нового процесса
        if line.startswith('Процесс:'):
            # Сохраняем предыдущий процесс, если он был рабочим
            if current_data and not is_controller:
                # Извлекаем нужные поля
                latency = current_data.get('Среднее latency')
                runtime = current_data.get('Средний runtime')
                tasks = current_data.get('Всего задач')
                voluntary = current_data.get('voluntary_ctxt_switches')
                nonvoluntary = current_data.get('nonvoluntary_ctxt_switches')
                avg_cpu_load = current_data.get('avg_cpu_load')
                avg_memory_mb = current_data.get('avg_memory_mb')
                if None not in (latency, tasks, voluntary, nonvoluntary):
                    workers_data.append({
                        'latency': float(latency),
                        'runtime': float(runtime),
                        'tasks': int(tasks),
                        'voluntary': int(voluntary),
                        'nonvoluntary': int(nonvoluntary),
                        'avg_cpu_load': float(avg_cpu_load),
                        'avg_memory_mb': float(avg_memory_mb),
                    })
            # Сбрасываем для нового процесса
            current_data = {}
            is_controller = False
            continue

        # Проверка на управляющий процесс
        if 'УПРАВЛЯЮЩИЙ ПРОЦЕСС' in line:
            is_controller = True

        if is_controller:
            continue

        # Извлечение пар ключ: значение
        match = key_value_re.match(line)
        if match:
            key = match.group(1).strip()
            value = match.group(2).strip()
            current_data[key] = value

    # Обработка последнего процесса
    if current_data and not is_controller:
        latency = current_data.get('Среднее latency')
        runtime = current_data.get('Средний runtime')
        tasks = current_data.get('Всего задач')
        voluntary = current_data.get('voluntary_ctxt_switches')
        nonvoluntary = current_data.get('nonvoluntary_ctxt_switches')
        avg_cpu_load = current_data.get('avg_cpu_load')
        avg_memory_mb = current_data.get('avg_memory_mb')
        if None not in (latency, tasks, voluntary, nonvoluntary):
            workers_data.append({
                'latency': float(latency),
                'runtime': float(runtime),
                'tasks': int(tasks),
                'voluntary': int(voluntary),
                'nonvoluntary': int(nonvoluntary),
                'avg_cpu_load': float(avg_cpu_load),
                'avg_memory_mb': float(avg_memory_mb),
            })

    if not workers_data:
        return None

    worker_count = len(workers_data)
    sum_latency = sum(w['latency'] for w in workers_data)
    sum_runtime = sum(w['runtime'] for w in workers_data)
    sum_tasks = sum(w['tasks'] for w in workers_data)
    sum_voluntary = sum(w['voluntary'] for w in workers_data)
    sum_nonvoluntary = sum(w['nonvoluntary'] for w in workers_data)
    sum_avg_cpu_load = sum(w['avg_cpu_load'] for w in workers_data)
    sum_avg_memory_mb = sum(w['avg_memory_mb'] for w in workers_data)

    return {
        'avg_latency': sum_latency / worker_count,
        'avg_runtime': sum_runtime / worker_count,
        'avg_tasks_per_worker': sum_tasks / worker_count,
        'sum_voluntary': sum_voluntary,
        'sum_nonvoluntary': sum_nonvoluntary,
        'avg_cpu_load': sum_avg_cpu_load / worker_count,
        'avg_memory_mb': sum_avg_memory_mb / worker_count,
        'sum_worker_proc': worker_count,
        'worker_count': worker_count
    }


def collect_for_combination(runs_root: Path) -> List[Dict]:
    """
    Обходит все поддиректории runs_root (например, 01_first, 02_second...),
    собирает для каждой данные из queue_throughput.log и summary.log.
    Возвращает список результатов для каждого прогона.
    """
    results = []
    # Сортируем для воспроизводимости порядка (по имени папки)
    for run_dir in sorted(runs_root.iterdir()):
        if not run_dir.is_dir():
            continue
        queue_file = run_dir / 'queue_throughput.log'
        summary_file = run_dir / 'summary.log'
        if not (queue_file.exists() and summary_file.exists()):
            print(f"  Пропускаем {run_dir.name}: отсутствуют необходимые файлы")
            continue

        # Данные из queue_throughput
        throughput, tasks_completed, duration = parse_queue_throughput(queue_file)

        # Данные из summary
        summary_stats = parse_summary(summary_file)
        if summary_stats is None:
            print(f"  Пропускаем {run_dir.name}: нет рабочих процессов в summary.log")
            continue

        results.append({
            'throughput': throughput,
            'tasks_completed': tasks_completed,
            'duration': duration,
            'avg_latency': summary_stats['avg_latency'],
            'avg_runtime': summary_stats['avg_runtime'],
            'avg_tasks_per_worker': summary_stats['avg_tasks_per_worker'],
            'sum_voluntary': summary_stats['sum_voluntary'],
            'sum_nonvoluntary': summary_stats['sum_nonvoluntary'],
            'avg_cpu_load': summary_stats['avg_cpu_load'],
            'avg_memory_mb': summary_stats['avg_memory_mb'],
            'worker_count': summary_stats['worker_count']
        })
    return results


def average_results(results: List[Dict]) -> Optional[Dict]:
    """Усредняет показатели по списку прогонов."""
    if not results:
        return None
    n = len(results)
    avg = {}
    for key in results[0].keys():
        avg[key] = sum(r[key] for r in results) / n
    avg['runs_count'] = n
    return avg


def compute_statistics(values: List[float]) -> Dict[str, float]:
    """Возвращает статистики для списка значений."""
    n = len(values)
    if n == 0:
        return {}
    mean = sum(values) / n
    # Выборочное стандартное отклонение
    variance = sum((x - mean) ** 2 for x in values) / (n - 1)
    std = math.sqrt(variance)
    # Стандартная ошибка среднего
    sem = std / math.sqrt(n)
    # 95% доверительный интервал (t-распределение)
    t_crit = t_critical(n, 0.95)
    ci_half = t_crit * sem
    median = sorted(values)[n//2]
    return {
        'mean': mean,
        'median': median,
        'std': std,
        'sem': sem,
        'ci_lower': mean - ci_half,
        'ci_upper': mean + ci_half,
        'cv': std / mean if mean != 0 else 0,
    }

def aggregate_results(results_list: List[Dict]) -> Dict:
    """
    Принимает список результатов dict (каждый dict содержит метрики одного прогона).
    Возвращает словарь с метриками, где вместо простого среднего – статистики.
    """
    if not results_list:
        return {}
    # Получаем список ключей (например, throughput, latency, ...)
    keys = results_list[0].keys()
    aggregated = {}
    for key in keys:
        values = [r[key] for r in results_list]
        aggregated[key] = compute_statistics(values)
    aggregated['runs_count'] = len(results_list)
    return aggregated


def main():
    default_root = '/home/experimenter/projects/celery-autoscale/docs/02_runtime/2026-04-26_01-02-40'
    parser = argparse.ArgumentParser(description='Сбор статистики тестов Celery')
    parser.add_argument('--root', default=default_root,
                        help='Корневая директория с папками *._processes (по умолчанию текущая)')
    parser.add_argument('--output', default=f'{default_root}/old_summary_report.txt',
                        help='Файл для сохранения отчёта (по умолчанию old_summary_report.txt)')
    args = parser.parse_args()

    root_path = Path(args.root).resolve()
    if not root_path.is_dir():
        print(f"Ошибка: директория {root_path} не существует")
        return

    # Сбор всех результатов по комбинациям
    report_lines = ["Сводная статистика тестов Celery", "=" * 60]

    for queue_type in QUEUE_TYPE.values():
        queue_dir = root_path / queue_type
        if not queue_dir.is_dir():
            continue

        # Ищем папки вида 02_processes, 04_processes и т.д.
        processes_dirs = sorted(queue_dir.glob('[0-9][0-9]_processes'))
        if not processes_dirs:
            print(f"Не найдено папок вида XX_processes в {queue_dir}")
            return

        for proc_dir in processes_dirs:
            # Извлекаем число процессов из имени папки (первые две цифры)
            proc_num = proc_dir.name[:2]
            # Проверяем наличие подпапок autoscale и concurrency
            for pool_type in WORKER_TYPE.values():
                pool_dir = proc_dir / pool_type
                if not pool_dir.is_dir():
                    continue

                print(f"Обработка: {proc_num} процессов, {pool_type}, {queue_type}")
                results = collect_for_combination(pool_dir)
                avg = average_results(results)
                aggregated = aggregate_results(results)
                if avg is None:
                    print(f"  Нет данных для этой комбинации")
                    continue

                runs_count = avg['runs_count']
                # Формируем строки отчёта
                header = f"{proc_num} рабочих процессов {pool_type} {queue_type}"
                report_lines.append("")
                report_lines.append(header)
                report_lines.append("-" * len(header))
                report_lines.append(f"Количество прогонов: {runs_count}")
                report_lines.append(f"Средняя длительность прогона: {avg['duration']:.2f}")
                report_lines.append(f"Среднее кол-во задач в прогоне: {avg['tasks_completed']:.2f}")
                report_lines.append("-" * len(header))
                report_lines.append(f"Среднее кол-во процессов: {avg['worker_count']:.2f}")
                report_lines.append(f"Среднее число выполненных задач на процесс: {avg['avg_tasks_per_worker']:.2f}")

                stats = aggregated['throughput']
                report_lines.append(f"Пропускная способность (throughput_tps):")
                report_lines.append(f"  среднее = {stats['mean']:.2f}, медиана = {stats['median']:.2f}")
                report_lines.append(f"  σ = {stats['std']:.2f}, CV = {stats['cv']:.2%}")
                report_lines.append(f"  95% ДИ = [{stats['ci_lower']:.2f} – {stats['ci_upper']:.2f}]")

                stats = aggregated['avg_latency']
                report_lines.append(f"Latency:")
                report_lines.append(f"  среднее = {stats['mean']:.2f}, медиана = {stats['median']:.2f}")
                report_lines.append(f"  σ = {stats['std']:.2f}, CV = {stats['cv']:.2%}")
                report_lines.append(f"  95% ДИ = [{stats['ci_lower']:.2f} – {stats['ci_upper']:.2f}]")

                stats = aggregated['avg_runtime']
                report_lines.append(f"Runtime:")
                report_lines.append(f"  среднее = {stats['mean']:.2f}, медиана = {stats['median']:.2f}")
                report_lines.append(f"  σ = {stats['std']:.2f}, CV = {stats['cv']:.2%}")
                report_lines.append(f"  95% ДИ = [{stats['ci_lower']:.2f} – {stats['ci_upper']:.2f}]")

                report_lines.append("-" * len(header))
                report_lines.append(f"Среднее использование ЦПУ: {avg['avg_cpu_load']:.2f}")
                report_lines.append(f"Среднее использование РАМ: {avg['avg_memory_mb']:.2f}")
                report_lines.append(f"Среднее voluntary_ctxt_switches: {avg['sum_voluntary'] / runs_count:.2f}")
                report_lines.append(f"Среднее nonvoluntary_ctxt_switches: {avg['sum_nonvoluntary'] / runs_count:.2f}")

    # Вывод отчёта
    report_text = "\n".join(report_lines)
    print("\n" + report_text)

    # Сохранение в файл
    output_file = Path(args.output)
    try:
        output_file.write_text(report_text, encoding='utf-8')
        print(f"\nОтчёт сохранён в {output_file}")
    except OSError as e:
        print(f"Ошибка при сохранении файла: {e}")


if __name__ == '__main__':
    main()
