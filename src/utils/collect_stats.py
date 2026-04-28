"""
Финальный сбор статистики тестов Celery из сырых логов задач.
Для каждой конфигурации (число процессов, тип пула, тип очереди) собирает:
- количество прогонов,
- среднее количество процессов (по числу уникальных лог-файлов),
- среднюю длительность, задачи, throughput,
- по логам задач: для каждого прогона средняя латентность, затем по прогонам:
    среднее, стандартное отклонение, CV, 95% ДИ,
- также общие перцентили (90,95) по всем задачам всех прогонов.
Вывод в формате Markdown.
Предупреждение: ⚠️ — коэффициент вариации (CV) > 30%.
"""

import argparse
import json
import math
import statistics
from pathlib import Path
from typing import Dict, List, Tuple, Optional

from tools.manipulate_path_dir import get_default_root


def t_critical(n: int, conf: float = 0.95) -> float:
    """
    Табличные значения t-распределения для двустороннего 95% ДИ.
    Если n <= 1, возвращает float('nan').
    """
    if n <= 1:
        return float('nan')
    table = {
        2: 4.303, 3: 3.182, 4: 2.776, 5: 2.571, 6: 2.447,
        7: 2.365, 8: 2.306, 9: 2.262, 10: 2.228, 11: 2.201,
        12: 2.179, 13: 2.160, 14: 2.145, 15: 2.131, 16: 2.120,
        17: 2.110, 18: 2.101, 19: 2.093, 20: 2.086, 21: 2.080,
        22: 2.074, 23: 2.069, 24: 2.064, 25: 2.060, 26: 2.056,
        27: 2.052, 28: 2.048, 29: 2.045, 30: 2.042, 31: 2.040,
        32: 2.037, 33: 2.035, 34: 2.032, 35: 2.030, 36: 2.028,
        37: 2.026, 38: 2.024, 39: 2.023, 40: 2.021, 41: 2.020,
        42: 2.018, 43: 2.017, 44: 2.015, 45: 2.014, 46: 2.013,
        47: 2.012, 48: 2.011, 49: 2.010, 50: 2.009, 51: 2.008,
        52: 2.007, 53: 2.006, 54: 2.005, 55: 2.004, 56: 2.003,
        57: 2.002, 58: 2.002, 59: 2.001, 60: 2.000, 61: 2.000,
        70: 1.994, 80: 1.990, 90: 1.987, 100: 1.984,
    }
    if n in table:
        return table[n]
    if n > 100:
        return 1.96
    # интерполяция для промежуточных n (например, 33, 34...)
    keys = sorted([k for k in table.keys() if k <= n])
    if not keys:
        return 1.96
    lower = max(keys)
    higher = min([k for k in table.keys() if k >= n], default=lower)
    if lower == higher:
        return table[lower]
    ratio = (n - lower) / (higher - lower)
    return table[lower] + ratio * (table[higher] - table[lower])


def read_queue_throughput(file_path: Path) -> Tuple[float, int, float]:
    """Возвращает (throughput_tps, tasks_completed, duration_sec) из queue_throughput.log"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError):
        return 0.0, 0, 0.0
    periods = data.get('active_periods', [])
    total_tasks = 0
    total_duration = 0.0
    for p in periods:
        total_tasks += p.get('tasks_completed', 0)
        total_duration += p.get('duration_sec', 0.0)
    throughput = total_tasks / total_duration if total_duration > 0 else 0.0
    return throughput, total_tasks, total_duration


def read_completed_tasks(run_dir: Path) -> Tuple[List[float], List[float], int]:
    """
    Читает все completed_tasks_*.log в run_dir.
    Возвращает (список латентностей всех задач, список runtime всех задач, количество уникальных процессов).
    Количество процессов = число файлов с префиксом completed_tasks_.
    """
    latencies = []
    runtimes = []
    process_files = list(run_dir.glob('completed_tasks_*.log'))
    num_processes = len(process_files)

    for log_file in process_files:
        try:
            with open(log_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        data = json.loads(line)
                        lat = data.get('duration_including_enqueue')
                        rt = data.get('duration_into_wraps')
                        if lat is not None:
                            latencies.append(float(lat))
                        if rt is not None:
                            runtimes.append(float(rt))
                    except json.JSONDecodeError:
                        continue
        except OSError:
            continue
    return latencies, runtimes, num_processes


def percentiles(values: List[float], percents: List[int]) -> Dict[int, float]:
    """Вычисляет перцентили без numpy."""
    if not values:
        return {p: 0.0 for p in percents}
    sorted_vals = sorted(values)
    n = len(sorted_vals)
    result = {}
    for p in percents:
        idx = (p / 100.0) * (n - 1)
        lower = int(idx)
        upper = lower + 1
        if upper >= n:
            result[p] = sorted_vals[-1]
        else:
            weight = idx - lower
            result[p] = (1 - weight) * sorted_vals[lower] + weight * sorted_vals[upper]
    return result


def analyze_run(run_dir: Path) -> Tuple[Optional[Dict], List[float]]:
    """
    Анализирует один прогон: собирает данные из queue_throughput и логов задач.
    Возвращает (словарь с метриками, список латентностей всех задач этого прогона).
    """
    queue_file = run_dir / 'queue_throughput.log'
    if not queue_file.exists():
        return None, []
    throughput, tasks, duration = read_queue_throughput(queue_file)
    if tasks == 0:
        return None, []
    latencies, runtimes, num_processes = read_completed_tasks(run_dir)
    if not latencies or not runtimes:
        return None, []

    mean_lat = statistics.mean(latencies)
    median_lat = statistics.median(latencies)
    perc_lat = percentiles(latencies, [90, 95])
    mean_rt = statistics.mean(runtimes)
    median_rt = statistics.median(runtimes)
    perc_rt = percentiles(runtimes, [90, 95])

    result = {
        'throughput': throughput,
        'tasks': tasks,
        'duration': duration,
        'mean_latency': mean_lat,
        'median_latency': median_lat,
        'p90_latency': perc_lat[90],
        'p95_latency': perc_lat[95],
        'mean_runtime': mean_rt,
        'median_runtime': median_rt,
        'p90_runtime': perc_rt[90],
        'p95_runtime': perc_rt[95],
        'num_processes': num_processes,
    }
    return result, latencies


def collect_for_combination(runs_root: Path) -> Tuple[List[Dict], List[float]]:
    """Собирает список результатов по всем прогонам в папке runs_root и все латентности."""
    results = []
    all_latencies = []
    for run_dir in sorted(runs_root.iterdir()):
        if not run_dir.is_dir():
            continue
        res, lats = analyze_run(run_dir)
        if res:
            results.append(res)
            all_latencies.extend(lats)
    return results, all_latencies


def compute_stats_over_runs(results: List[Dict]) -> Dict:
    """
    По списку результатов прогонов вычисляет статистики:
        n, avg_duration, avg_tasks, avg_throughput,
        mean_latency (среднее средних), sd_latency, cv_latency (в %),
        ci_lower, ci_upper, avg_num_processes.
    """
    if not results:
        return {}
    n = len(results)
    lat_means = [r['mean_latency'] for r in results]
    mean_of_means = statistics.mean(lat_means)

    if n > 1:
        stdev = statistics.stdev(lat_means)
        cv = (stdev / mean_of_means) * 100.0 if mean_of_means != 0 else 0.0
        sem = stdev / math.sqrt(n)
        t_crit = t_critical(n, 0.95)
        if math.isnan(t_crit):
            ci_lower = ci_upper = float('nan')
        else:
            ci_half = t_crit * sem
            ci_lower = mean_of_means - ci_half
            ci_upper = mean_of_means + ci_half
    else:
        stdev = 0.0
        cv = 0.0
        ci_lower = ci_upper = float('nan')

    avg_num_processes = statistics.mean(r['num_processes'] for r in results)

    return {
        'n': n,
        'avg_duration': statistics.mean(r['duration'] for r in results),
        'avg_tasks': round(statistics.mean(r['tasks'] for r in results), 1),
        'avg_throughput': statistics.mean(r['throughput'] for r in results),
        'mean_latency': mean_of_means,
        'sd_latency': stdev,
        'cv_latency': cv,
        'ci_lower': ci_lower,
        'ci_upper': ci_upper,
        'avg_num_processes': avg_num_processes,
    }


def main():
    parser = argparse.ArgumentParser(description='Сбор статистики тестов Celery')
    parser.add_argument('--root', default=get_default_root(),
                        help='Корневая директория, содержащая папки-таймстампы (по умолчанию docs/02_runtime)')
    parser.add_argument('--output', default=f'{get_default_root()}/summary_report.txt',
                        help='Файл для сохранения отчёта')
    parser.add_argument('--non-interactive', action='store_true',
                        help='Неинтерактивный режим: при наличии нескольких таймстампов берётся последний')
    args = parser.parse_args()

    root_path = Path(args.root).resolve()
    if not root_path.is_dir():
        print(f"Ошибка: директория {root_path} не существует")
        return

    # --- НАЧАЛО БЛОКА ВЫБОРА ТАЙМСТАМПА ---
    # Собираем все поддиректории в root_path (игнорируем скрытые и файлы)
    timestamps = [d for d in root_path.iterdir() if d.is_dir() and not d.name.startswith('.')]
    timestamps.sort()  # сортировка по имени даёт хронологический порядок

    if not timestamps:
        print(f"Ошибка: в {root_path} не найдено ни одной папки-таймстампа")
        return

    if len(timestamps) == 1:
        selected = timestamps[0]
        print(f"Найден один таймстамп: {selected.name}. Использую его.")
    else:
        if args.non_interactive:
            selected = timestamps[-1]  # последний (самый новый, т.к. сортировка лексикографическая)
            print(f"Неинтерактивный режим. Выбран последний таймстамп: {selected.name}")
        else:
            print("Найдены следующие таймстампы:")
            for i, ts in enumerate(timestamps, start=1):
                # Можно добавить дополнительную информацию, например количество вложенных элементов
                print(f"{i}. {ts.name}")
            while True:
                try:
                    choice = input("Введите номер таймстампа для анализа: ").strip()
                    idx = int(choice) - 1
                    if 0 <= idx < len(timestamps):
                        selected = timestamps[idx]
                        break
                    else:
                        print(f"Номер должен быть от 1 до {len(timestamps)}")
                except ValueError:
                    print("Пожалуйста, введите число")

    # Теперь root_path указывает на выбранную папку с таймстампом
    root_path = selected
    print(f"Рабочая директория: {root_path}")
    # --- КОНЕЦ БЛОКА ВЫБОРА ---

    # Далее идёт ваша существующая логика сбора статистики, использующая root_path
    report_lines = ["Сводная статистика тестов Celery", "=" * 60]
    # ... остальной код

    lines = ["# Итоговая статистика тестов Celery (по сырым логам задач)",
             "",
             "## Накопительная нагрузка (cumulative)",
             ""]
    header = ("| Процессов (зад.) | Режим | Прогонов | Ср. процессов | Длительность (с) | Задач | Throughput (з/с) | "
              "Сред. latency (с) | CV latency | 95% ДИ для среднего | Медиана (с)* | 90-й перц. (с)* | 95-й перц. (с)* | Прим. |")
    sep = "|-----------------|-------|----------|---------------|------------------|-------|------------------|-------------------|------------|----------------------|--------------|-----------------|-----------------|-------|"
    lines.append(header)
    lines.append(sep)

    for queue_type in ['cumulative', 'schedule']:
        if queue_type == 'schedule':
            lines.append("")
            lines.append("## Равномерная нагрузка по расписанию (schedule)")
            lines.append("")
            lines.append(header)
            lines.append(sep)
        queue_dir = root_path / queue_type
        if not queue_dir.is_dir():
            continue
        processes_dirs = sorted(queue_dir.glob('[0-9][0-9]_processes'))
        for proc_dir in processes_dirs:
            proc_num = proc_dir.name[:2]
            for pool_type in ['autoscale', 'concurrency']:
                pool_dir = proc_dir / pool_type
                if not pool_dir.is_dir():
                    continue
                results, all_lat = collect_for_combination(pool_dir)
                if not results:
                    continue
                stats = compute_stats_over_runs(results)
                n = stats['n']
                avg_num_proc = stats['avg_num_processes']
                dur = stats['avg_duration']
                tasks = stats['avg_tasks']
                thr = stats['avg_throughput']
                mean_lat = stats['mean_latency']
                cv = stats['cv_latency']
                ci_low = stats['ci_lower']
                ci_high = stats['ci_upper']

                if all_lat:
                    median_all = statistics.median(all_lat)
                    perc_all = percentiles(all_lat, [90, 95])
                    p90 = perc_all[90]
                    p95 = perc_all[95]
                else:
                    median_all = p90 = p95 = 0.0

                # Флаги предупреждений
                flags = []
                if cv > 30:
                    flags.append("⚠️")
                # Аномалия только если разница относительная > 5% и абсолютная > 0.1 секунды
                if stats['sd_latency'] > 0 and abs(mean_lat - median_all) > max(0.05 * mean_lat, 0.1):
                    flags.append("🔴")
                flag_str = " ".join(flags) if flags else ""

                ci_str = f"[{ci_low:.2f} – {ci_high:.2f}]" if not math.isnan(ci_low) else "N/A"
                row = (f"| {proc_num} | {pool_type} | {n} | {avg_num_proc:.2f} | {dur:.2f} | {tasks:.1f} | {thr:.2f} | "
                       f"{mean_lat:.2f} | {cv:.1f}% | {ci_str} | {median_all:.2f} | {p90:.2f} | {p95:.2f} | {flag_str} |")
                lines.append(row)

    lines.append("")
    lines.append("> * — общие перцентили по всем задачам всех прогонов. ")
    lines.append("> ⚠️ — коэффициент вариации (CV) > 30%.")
    lines.append("")

    output_text = "\n".join(lines)
    print(output_text)
    output_path = Path(args.output)
    output_path.write_text(output_text, encoding='utf-8')
    print(f"\nОтчёт сохранён в {output_path}")


if __name__ == '__main__':
    main()