import os
import re
import glob
import csv
from collections import defaultdict

from utils.args import QUEUE_TYPE, WORKER_TYPE


def parse_filename(filename):
    pattern = r'worker_celery_(exp_\d+)_([aceg])_([sv])_(\d+)_iter\d+_\d+\.log'
    match = re.match(pattern, os.path.basename(filename))
    if not match:
        return None
    exp_id, launch_type, queue_type, max_workers = match.groups()
    return {
        'exp_id': exp_id,
        'launch_type': launch_type,
        'queue_type': queue_type,
        'max_workers': int(max_workers),
        'min_workers': int(max_workers) // 2 if launch_type == 'a' else None,
        'filename': filename
    }


def extract_times(filepath):
    times = []
    with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
        for line in f:
            if 'succeeded in' in line:
                m = re.search(r'succeeded in ([\d\.]+)s', line)
                if m:
                    times.append(float(m.group(1)))
    return times


def percentile(sorted_data, p):
    """Вычисляет p-й перцентиль (p от 0 до 100) по отсортированному списку"""
    if not sorted_data:
        return None
    n = len(sorted_data)
    idx = (p / 100.0) * (n - 1)
    lower = int(idx)
    upper = lower + 1
    if upper >= n:
        return sorted_data[lower]
    weight = idx - lower
    return sorted_data[lower] * (1 - weight) + sorted_data[upper] * weight


def main(directory='/home/experimenter/projects/celery-autoscale/src/run_scripts/worker_logs', output_csv='stats_summary_runtime.csv'):
    log_files = glob.glob(os.path.join(directory, '*.log'))
    if not log_files:
        print(f"Нет файлов .log в {directory}")
        return

    # Группировка: ключ -> список времён, множество итераций
    groups = defaultdict(lambda: {'times': [], 'iterations': set()})
    for fpath in log_files:
        info = parse_filename(fpath)
        if info is None:
            print(f"Пропущен файл (не распознан): {fpath}")
            continue
        key = (info['exp_id'], info['launch_type'], info['queue_type'], info['max_workers'])
        times = extract_times(fpath)
        groups[key]['times'].extend(times)
        groups[key]['iterations'].add(info['filename'])
        groups[key]['min_workers'] = info['min_workers']

    results = []
    for (exp_id, launch_type, queue_type, max_workers), data in groups.items():
        times = data['times']
        if not times:
            print(f"Нет задач для {exp_id}_{launch_type}_{queue_type}_{max_workers}")
            continue
        times_sorted = sorted(times)
        med = percentile(times_sorted, 50)
        p95 = percentile(times_sorted, 95)
        p99 = percentile(times_sorted, 99)
        workers_str = f"{max_workers},{data['min_workers']}" if launch_type == 'a' else str(max_workers)
        results.append({
            'exp_id': exp_id,
            'режим': WORKER_TYPE.get(launch_type, 'N/A'),
            'очередь': QUEUE_TYPE.get(queue_type, 'N/A'),
            'воркеры': workers_str,
            'медиана (50)': round(med, 4),
            '95-й перцентиль': round(p95, 4),
            '99-й перцентиль': round(p99, 4),
            'кол-во задач': len(times),
            'кол-во итераций': len(data['iterations'])
        })

    if not results:
        print("Нет результатов для вывода.")
        return

    # Сортировка
    results.sort(key=lambda x: (x['exp_id'], x['режим'], x['воркеры'], x['очередь']))

    # Вывод таблицы в консоль
    print("\n=== Сводная статистика ===\n")
    header = ["exp_id", "режим", "очередь", "воркеры", "медиана(50)", "95%", "99%", "задач", "итераций"]
    print("{:<12} {:<10} {:<10} {:<8} {:>10} {:>10} {:>10} {:>8} {:>8}".format(*header))
    for r in results:
        print("{:<12} {:<10} {:<10} {:<8} {:>10.4f} {:>10.4f} {:>10.4f} {:>8} {:>8}".format(
            r['exp_id'], r['режим'], r['очередь'], r['воркеры'], r['медиана (50)'], r['95-й перцентиль'],
            r['99-й перцентиль'], r['кол-во задач'], r['кол-во итераций']
        ))

    # Сохраняем CSV
    with open(output_csv, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=results[0].keys())
        writer.writeheader()
        writer.writerows(results)
    print(f"\nРезультат сохранён в {output_csv}")


if __name__ == '__main__':
    import sys

    if len(sys.argv) > 1:
        main(directory=sys.argv[1])
    else:
        main()
