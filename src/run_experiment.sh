#!/bin/bash
set -euo pipefail
#set -x

# -------------------- Путь к venv --------------------
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
SLEEP_BETWEEN=2

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

WORKER_PID=""

# -------------------- Функции --------------------
error_exit() {
    echo -e "${RED}Ошибка: $1${NC}" >&2
    exit 1
}

stop_worker() {
    if [[ -n "$WORKER_PID" ]]; then
        if kill -0 "$WORKER_PID" 2>/dev/null; then
            echo -e "${YELLOW}Останавливаем worker (PID $WORKER_PID)...${NC}"
            kill -TERM "$WORKER_PID" 2>/dev/null || true
            for _ in {1..10}; do
                kill -0 "$WORKER_PID" 2>/dev/null || break
                sleep 1
            done
            if kill -0 "$WORKER_PID" 2>/dev/null; then
                echo -e "${RED}Worker не завершился, принудительно убиваем${NC}"
                kill -9 "$WORKER_PID" 2>/dev/null || true
            fi
            wait "$WORKER_PID" 2>/dev/null || true
        fi
        WORKER_PID=""
    fi
}

cleanup() {
    echo -e "${YELLOW}Очистка...${NC}"
    stop_worker
    if [[ -d "/sys/fs/cgroup/$CGROUP_NAME" ]]; then
        sudo rmdir "/sys/fs/cgroup/$CGROUP_NAME" 2>/dev/null || true
    fi
}

start_worker() {
    echo -e "${GREEN}Запускаем Celery worker (лог: $WORKER_LOG)${NC}"
    $CELERY_EXE -A "$CELERY_APP" worker --loglevel=INFO \
        "$PREFORK_ARG" > "$WORKER_LOG" 2>&1 &
    WORKER_PID=$!
    echo "$WORKER_PID" | sudo tee "/sys/fs/cgroup/$CGROUP_NAME/cgroup.procs" >/dev/null

    sleep 2
    if ! kill -0 "$WORKER_PID" 2>/dev/null; then
        echo -e "${RED}Worker упал при старте. Последние строки лога:${NC}"
        tail -20 "$WORKER_LOG"
        error_exit "Worker не запустился."
    fi
    echo -e "${GREEN}Worker запущен, PID: $WORKER_PID${NC}"
}

# -------------------- Проверка cgroups v2 --------------------
if ! mount | grep -q "cgroup2 on /sys/fs/cgroup"; then
    error_exit "cgroups v2 не смонтированы."
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

MAX_PROC_NUM=$((10#$MAX_PROC))
if [[ "$WORKER_TYPE" == "a" ]]; then
    MIN_PROC=$((MAX_PROC_NUM / 2))
    PREFORK_ARG="--autoscale=$MAX_PROC_NUM,$MIN_PROC"
else
    PREFORK_ARG="--concurrency=$MAX_PROC_NUM"
fi
# -------------------- Создание cgroup --------------------
echo -e "${GREEN}Создаём cgroup '$CGROUP_NAME' с ограничением в 2 ядра...${NC}"
sudo mkdir -p "/sys/fs/cgroup/$CGROUP_NAME"
echo "+cpu +cpuset" | sudo tee "/sys/fs/cgroup/cgroup.subtree_control" >/dev/null || true
echo "$CPU_QUOTA $CPU_PERIOD" | sudo tee "/sys/fs/cgroup/$CGROUP_NAME/cpu.max" >/dev/null

if [[ "$USE_CPUSET" == "yes" ]]; then
    echo "0-1" | sudo tee "/sys/fs/cgroup/$CGROUP_NAME/cpuset.cpus" >/dev/null
    echo "0"   | sudo tee "/sys/fs/cgroup/$CGROUP_NAME/cpuset.mems" >/dev/null
fi

trap cleanup EXIT INT TERM

# -------------------- Цикл экспериментов --------------------
echo -e "${GREEN}Начинаем серию из 15 экспериментов. На каждой итерации worker перезапускается.${NC}"

for order in {1..15}; do
    echo -e "${YELLOW}--- Запуск $order из 15 ---${NC}"

    start_worker

    $PYTHON_EXE "$PYTHON_SCRIPT" "$WORKER_TYPE" "$MAX_PROC" "$QUEUE_TYPE" "$order"

    sleep "$SLEEP_BETWEEN"

    stop_worker

done

echo -e "${GREEN}Все 15 экспериментов завершены. Результаты в $LOG_DIR_BASE/${NC}"
cleanup