import os
from datetime import datetime
from pathlib import Path


def get_default_root() -> Path:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(script_dir))  # дважды подняться
    return Path(project_root) / 'docs' / '02_runtime'


def create_path(max_proc_str: str, worker_dir: str, queue_dir: str, order_dir: str) -> str:
    """
    Безопасное создание директории для хранения логов в один запуск.
    Если прежде подобный прогон был, то архивируем по типу очереди(schedule/cumulative),
    добавляя суффикс _old к существующей директории
    """
    logs_path = get_default_root().resolve()
    timestamp = os.environ.get('EXPERIMENT_TIMESTAMP') or datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    target_dir = logs_path / timestamp / queue_dir/ f"{max_proc_str}_processes" / worker_dir / order_dir

    try:
        target_dir.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        raise OSError(f"Не удалось создать директорию {target_dir}: {e}")

    return str(target_dir)
