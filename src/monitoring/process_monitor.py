import json
import os
import time

import aiofiles
import psutil

from datetime import datetime
from collections import defaultdict

from tools.read_proc_dir import get_process_info


class ProcessMonitor:

    __slots__ = ['celery_processes', 'file_path']

    def __init__(self, file_path: str):
        self.celery_processes = defaultdict(dict)
        self.file_path = file_path

    def update_stats(self):
        for proc in psutil.process_iter(['pid', 'name', 'cmdline', 'memory_info', 'cpu_percent', 'create_time']):
            try:
                cmdline = proc.info['cmdline'] or []
                cmd_str = ' '.join(cmdline)

                if 'celery' in cmd_str and 'worker' in cmd_str:
                    pid = proc.info['pid']
                    proc_info = get_process_info(pid)
                    cur_time = time.time()
                    if pid not in self.celery_processes.keys():
                        self.celery_processes[pid] = {
                            'ppid': proc_info['ppid'],
                            'name': proc.info['name'],
                            'cmdline': cmd_str,
                            'create_time': datetime.fromtimestamp(proc.info['create_time']).isoformat() if proc.info[
                                'create_time'] else None,
                            'last_update': datetime.fromtimestamp(cur_time).isoformat(),
                            'voluntary_ctxt_switches': proc_info['voluntary_ctxt_switches'],
                            'nonvoluntary_ctxt_switches': proc_info['nonvoluntary_ctxt_switches'],
                            'jiffies': proc_info['jiffies'],
                            'stats': [],
                        }

                    self.celery_processes[pid].update({
                        'last_update': datetime.fromtimestamp(cur_time).isoformat(),
                        'voluntary_ctxt_switches': proc_info['voluntary_ctxt_switches'],
                        'nonvoluntary_ctxt_switches': proc_info['nonvoluntary_ctxt_switches'],
                        'jiffies': proc_info['jiffies'],
                    })

                    self.celery_processes[pid]['stats'].append({
                        'memory_mb': proc.info['memory_info'].rss / 1024 / 1024 if proc.info['memory_info'] else 0,
                        'cpu_percent': proc.info['cpu_percent'],
                        'timestamp': cur_time
                    })
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue

    async def _write_to_file(self, data_to_log, file_name):
        _file_path = os.path.join(self.file_path, file_name)
        async with aiofiles.open(_file_path, "a", encoding='utf-8') as log_file:
            if isinstance(data_to_log, list):
                for item in data_to_log:
                    await log_file.write(f"{item}\n")
            else:
                await log_file.write(json.dumps(data_to_log,
                                                indent=4,
                                                ensure_ascii=False) + "\n")

    async def _read_log_file(self, pid: int) -> dict | None:
        file_path = os.path.join(self.file_path, f"completed_tasks_{pid}.log")

        durations = []
        durations_include_q = []
        task_counts = {}
        total_tasks = 0

        try:
            async with aiofiles.open(file_path, 'r') as f:
                async for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        data = json.loads(line)
                        durations.append(data.get('duration_into_wraps', 0.0))
                        durations_include_q.append(data.get('duration_including_enqueue', 0.0))

                        task_name = data.get('task_name')
                        if task_name:
                            task_counts[task_name] = task_counts.get(task_name, 0) + 1

                        total_tasks += 1
                    except json.JSONDecodeError:
                        continue
        except FileNotFoundError:
            return None

        avg_task_runtime = sum(durations) / len(durations) if durations else 0.0
        avg_latency = sum(durations_include_q) / len(durations_include_q) if durations_include_q else 0.0

        return {
            'avg_task_runtime': round(avg_task_runtime, 2),
            'avg_latency': round(avg_latency, 2),
            'total_tasks': total_tasks,
            'tasks_by_type': task_counts
        }

    @staticmethod
    def _calc_avg(data):
        cpus = []
        memories = []
        for da in data:
            cpus.append(da['cpu_percent'])
            memories.append(da['memory_mb'])

        avg_cpu_load = sum(cpus) / len(cpus) if cpus else 0.0
        avg_memory_mb = sum(memories) / len(memories) if memories else 0.0

        return avg_cpu_load, avg_memory_mb

    async def format_stats(self) -> str:
        """
        Пишем в файл сырую статистику для дебага.

        Потом форматируем и выводим итоги по работе процессов в summary.log
        """
        await self._write_to_file(self.celery_processes,  "processes_stats.log")
        lines = []
        for key, value in self.celery_processes.items():
            stats_by_tasks = await self._read_log_file(key)
            avg_cpu_load, avg_memory_mb = self._calc_avg(value['stats'])

            lines += [
                f"Процесс: {key}",
                f"Родитель: {value['ppid']}",
                f"Создан: {value.get('create_time', 'N/A')}",
                f"Последний замер: {value.get('last_update', 'N/A')}",
                f"Время жизни: {(datetime.fromisoformat(value['last_update']) 
                                 - datetime.fromisoformat(value['create_time'])).total_seconds()} сек",
                f"voluntary_ctxt_switches: {value.get('voluntary_ctxt_switches', 0)}",
                f"nonvoluntary_ctxt_switches: {value.get('nonvoluntary_ctxt_switches', 0)}",
                f"jiffies: {value.get('jiffies', 0):.1f}",
                f"avg_cpu_load: {avg_cpu_load:.1f} %",
                f"avg_memory_mb: {avg_memory_mb:.1f} MB",
                ""
                 ]

            if stats_by_tasks is None:
                lines.insert(1, 'УПРАВЛЯЮЩИЙ ПРОЦЕСС')
            else:
                lines += [
                    f"Среднее latency: {stats_by_tasks['avg_latency']}",
                    f"Средний runtime: {stats_by_tasks['avg_task_runtime']}",
                    f"Всего задач: {stats_by_tasks['total_tasks']}",
                    "Задач по типам: " + "; ".join(f"{k} - {v}" for k, v in stats_by_tasks['tasks_by_type'].items())
                ]

            lines += [""]

        await self._write_to_file(lines, "summary.log")

        return "\n".join(lines)
