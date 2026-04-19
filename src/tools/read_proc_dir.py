import os


def _read_proc_file(path: str) -> str | None:
    try:
        with open(path, 'r') as f:
            return f.read().strip()
    except (IOError, OSError):
        return None


def _parse_status_field(status_text: str, field: str) -> str | None:
    """
    Извлекает значение поля из /proc/[pid]/status
    """
    if not status_text:
        return None
    for line in status_text.splitlines():
        if line.startswith(field + ':'):
            return line.split(':', 1)[1].strip()
    return None


def _get_threads_info(pid: int) -> list:
    """
    Читаем потоки целевого процесса.

    В общем, сгодится для последующего изучения пула threads,
    так как для prefork поток по умолчанию один; многопоточность начинается только в режиме дебага в IDE.
    """
    threads = []
    task_dir = f"/proc/{pid}/task"
    if not os.path.isdir(task_dir):
        return threads
    for tid_str in os.listdir(task_dir):
        try:
            tid = int(tid_str)
            status = _read_proc_file(f"{task_dir}/{tid}/status")
            wchan = _read_proc_file(f"{task_dir}/{tid}/wchan")
            stack = _read_proc_file(f"{task_dir}/{tid}/stack")
            state = _parse_status_field(status, 'State')
            state_code = state[0] if state else '?'
            threads.append({
                'tid': tid,
                'state': state_code,
                'wchan': wchan,
                'stack': stack,
            })
        except (ValueError, OSError):
            continue
    return threads


def _get_cpu(pid: int) -> int:
    with open(f"/proc/{pid}/stat") as f:
        parts = f.read().split()
    utime = int(parts[13])
    stime = int(parts[14])
    # общее количество процессорных тиков (jiffies), затраченных процессом с момента запуска
    jiffies = utime + stime
    return jiffies


def _get_memory_in_mib(status) -> float:
    return round(int(_parse_status_field(status, 'VmRSS').split(' ')[0]) / 1024, 2)


def get_process_info(pid: int) -> dict:
    """
    Получаем информацию о жизни рабочих процессов из директории /proc/{pid}

    """
    status = _read_proc_file(f"/proc/{pid}/status")
    ppid = _parse_status_field(status, 'PPid')
    voluntary_sw = _parse_status_field(status, 'voluntary_ctxt_switches')
    nonvoluntary_sw = _parse_status_field(status, 'nonvoluntary_ctxt_switches')
    jiffies = _get_cpu(pid)

    return {
        'ppid': ppid,
        'voluntary_ctxt_switches': int(voluntary_sw) if voluntary_sw else 0,
        'nonvoluntary_ctxt_switches': int(nonvoluntary_sw) if nonvoluntary_sw else 0,
        'jiffies': jiffies,
    }
