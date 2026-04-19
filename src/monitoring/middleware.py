import json
import os
from functools import wraps

import time

_task_log_files = {}


def __get_log_file(pid: int, file_path: str):
    if pid not in _task_log_files:
        _file_path = os.path.join(file_path, f"completed_tasks_{pid}.log")
        f = open(_file_path, "a")
        _task_log_files[pid] = f
        import atexit
        atexit.register(lambda: f.close())

    return _task_log_files[pid]


def os_stats(func):
    @wraps(func)
    def decorated_function(*args, **kwargs):
        """
        Подсчета задач внутри одного рабочего процесса.
        """
        pid = os.getpid()
        task_name = args[0].name
        ts_before = time.time()

        result = func(*args, **kwargs)

        enqueue_time = args[3]
        ts_after = time.time()
        file_path = args[2]

        log_line = json.dumps({
            'idx_task': args[1],
            "task_name": task_name,
            "duration_into_wraps": round(ts_after - ts_before, 2),
            "duration_including_enqueue": round(ts_after - enqueue_time, 2),
        }, ensure_ascii=False) + "\n"

        log_path = os.path.join(file_path, f"completed_tasks_{pid}.log")
        with open(log_path, "a", encoding='utf-8') as f:
            f.write(log_line)

        return result

    return decorated_function
