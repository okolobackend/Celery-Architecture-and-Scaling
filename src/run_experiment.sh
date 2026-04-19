#!/bin/bash
set -euo pipefail
#set -x

# -------------------- Определяем путь к venv --------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
VENV_PATH="${PROJECT_ROOT}/.venv"

if [[ ! -d "$VENV_PATH" ]]; then
    echo "Ошибка: виртуальное окружение не найдено по пути $VENV_PATH" >&2
    exit 1
fi

PYTHON_EXE="${VENV_PATH}/bin/python"
CELERY_EXE="${VENV_PATH}/bin/celery"

if [[ ! -x "$PYTHON_EXE" ]] || [[ ! -x "$CELERY_EXE" ]]; then
    echo "Ошибка: Python или Celery не найдены в venv" >&2
    exit 1
fi

# -------------------- Конфигурация --------------------
USE_CPUSET="no"   # или "yes"
CGROUP_NAME="celery_exp_$$"
CPU_QUOTA="200000"
CPU_PERIOD="100000"
CELERY_APP="celery_app"
PYTHON_SCRIPT="${SCRIPT_DIR}/main.py"
LOG_DIR_BASE="../docs/02_runtime"
WORKER_LOG="${SCRIPT_DIR}/worker.log"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

function error_exit {
    echo -e "${RED}Ошибка: $1${NC}" >&2
    cleanup
    exit 1
}

function cleanup {
    echo -e "${YELLOW}Завершаем worker и удаляем cgroup...${NC}"
    if [[ -n "${WORKER_PID:-}" ]] && kill -0 "$WORKER_PID" 2>/dev/null; then
        kill -TERM "$WORKER_PID" 2>/dev/null || true
        wait "$WORKER_PID" 2>/dev/null || true
    fi
    if [[ -d "/sys/fs/cgroup/$CGROUP_NAME" ]]; then
        sudo rmdir "/sys/fs/cgroup/$CGROUP_NAME" 2>/dev/null || true
    fi
}

trap cleanup EXIT

# Проверка cgroups v2
if ! mount | grep -q "cgroup2 on /sys/fs/cgroup"; then
    error_exit "cgroups v2 не смонтированы. Настройте систему."
fi

# -------------------- Разбор аргументов --------------------
WORKER_TYPE=""
QUEUE_TYPE=""
MAX_PROC=""

if [[ $# -eq 3 ]]; then
    WORKER_TYPE="$1"
    QUEUE_TYPE="$2"
    MAX_PROC="$3"
else
    echo "Введите параметры эксперимента:"
    echo "Тип worker'а (a=autoscale, c=concurrency):"
    read -r WORKER_TYPE
    echo "Тип очереди (v=накопительная, m=монотонная):"
    read -r QUEUE_TYPE
    echo "Максимум процессов (2, 4, 8):"
    read -r MAX_PROC
fi

if [[ ! "$WORKER_TYPE" =~ ^[ac]$ ]]; then
    error_exit "Неверный тип worker'а. Допустимо: a, c"
fi
if [[ ! "$QUEUE_TYPE" =~ ^[vm]$ ]]; then
    error_exit "Неверный тип очереди. Допустимо: v, m"
fi
if [[ ! "$MAX_PROC" =~ ^(2|4|8)$ ]]; then
    error_exit "Неверное число процессов. Допустимо: 2, 4, 8"
fi

# Это точно-точно десятичное число для максимума рабочих процессов
MAX_PROC_NUM=$((10#$MAX_PROC))

# -------------------- Создание cgroup --------------------
echo -e "${GREEN}Создаём cgroup '$CGROUP_NAME' с ограничением в 2 ядра...${NC}"
sudo mkdir -p "/sys/fs/cgroup/$CGROUP_NAME"
echo "+cpu +cpuset" | sudo tee "/sys/fs/cgroup/cgroup.subtree_control" >/dev/null || true
echo "$CPU_QUOTA $CPU_PERIOD" | sudo tee "/sys/fs/cgroup/$CGROUP_NAME/cpu.max" >/dev/null

if [[ "$USE_CPUSET" == "yes" ]]; then
    echo "0-1" | sudo tee "/sys/fs/cgroup/$CGROUP_NAME/cpuset.cpus" >/dev/null
    echo "0"   | sudo tee "/sys/fs/cgroup/$CGROUP_NAME/cpuset.mems" >/dev/null
fi
# -------------------- Запуск Celery worker --------------------
echo -e "${GREEN}Запускаем Celery worker (логи в $WORKER_LOG)${NC}"
$CELERY_EXE -A "$CELERY_APP" worker --loglevel=INFO \
    $([[ "$WORKER_TYPE" == "a" ]] && echo "--autoscale=$MAX_PROC_NUM,0" || echo "--concurrency=$MAX_PROC_NUM") \
    > "$WORKER_LOG" 2>&1 &
WORKER_PID=$!
echo "$WORKER_PID" | sudo tee "/sys/fs/cgroup/$CGROUP_NAME/cgroup.procs" >/dev/null

# Ждём и проверяем, жив ли worker
sleep 2
if ! kill -0 "$WORKER_PID" 2>/dev/null; then
    echo -e "${RED}Worker упал при старте. Последние строки лога:${NC}"
    tail -20 "$WORKER_LOG"
    error_exit "Worker не запустился."
fi

# -------------------- Проверка CPU --------------------
echo -e "${GREEN}Проверяем настройки CPU для cgroup:${NC}"
echo "Процесс воркера: $WORKER_PID"
echo -n "  - Разрешённые ядра (taskset): "
taskset -cp "$WORKER_PID" 2>/dev/null | cut -d ' ' -f 6- || echo "Не удалось определить"
echo -n "  - Доступные ядра (cpuset.cpus.effective): "
sudo cat "/sys/fs/cgroup/$CGROUP_NAME/cpuset.cpus.effective" 2>/dev/null || echo "Ограничений нет"
echo -n "  - CPU квота (cpu.max): "
sudo cat "/sys/fs/cgroup/$CGROUP_NAME/cpu.max"

# -------------------- Цикл экспериментов --------------------
echo -e "${GREEN}Начинаем серию из 15 экспериментов...${NC}"
for order in {1..15}; do
    echo -e "${YELLOW}--- Запуск $order из 15 ---${NC}"

    $PYTHON_EXE "$PYTHON_SCRIPT" "$WORKER_TYPE" "$MAX_PROC" "$QUEUE_TYPE" "$order"

    if [[ $? -ne 0 ]]; then
        echo -e "${RED}Python скрипт завершился с ошибкой. Прерываем серию.${NC}"
        break
    fi

    sleep 2
done

echo -e "${GREEN}Эксперимент завершён. Результаты в $LOG_DIR_BASE/${NC}"
