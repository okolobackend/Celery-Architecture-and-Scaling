from pathlib import Path


def create_path(max_proc_str: str, worker_dir: str, queue_dir: str, order_dir: str) -> str:
    """
    Безопасное создание директории для хранения логов в один запуск.
    Если прежде подобный прогон был, то архивируем по типу очереди(schedule/cumulative),
    добавляя суффикс _old к существующей директории
    """
    cur_dir = Path.cwd()
    parent_dir = cur_dir.parent

    base_path = parent_dir / 'docs' / '02_runtime' / queue_dir/ f"{max_proc_str}_processes" / worker_dir
    target_dir = base_path / order_dir

    # Если целевая папка существует, то перемещаем всю очередь, добавляя _old, _old_old и т.д.
    # Так как вероятно это повторный запуск
    if target_dir.exists():
        archive = base_path.parent / (base_path.name + '_old')
        while archive.exists():
            archive = archive.parent / (archive.name + '_old')
        base_path.rename(archive)

    # Приводим к абсолютному пути и разрешаем все симлинки/..
    resolved_target = target_dir.resolve()
    resolved_parent = parent_dir.resolve()

    # Проверяем, что конечная папка действительно внутри parent_dir
    if not resolved_target.is_relative_to(resolved_parent):
        raise PermissionError(f"Попытка выйти за пределы родительской директории: {target_dir}")

    try:
        resolved_target.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        raise OSError(f"Не удалось создать директорию {resolved_target}: {e}")

    return str(resolved_target)
