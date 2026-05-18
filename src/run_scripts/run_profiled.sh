#!/bin/bash
set -euo pipefail

EXPERIMENT_TIMESTAMP="${EXPERIMENT_TIMESTAMP:-$(date +"%Y-%m-%d_%H-%M-%S")}"
export EXPERIMENT_TIMESTAMP

# -------------------- Путь к venv --------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
SRC_DIR="${PROJECT_ROOT}/src"
VENV_PATH="${PROJECT_ROOT}/.venv"

if [[ ! -d "$VENV_PATH" ]]; then
    echo "Ошибка: виртуальное окружение не найдено по пути $VENV_PATH" >&2
    exit 1
fi

PYTHON_EXE="${VENV_PATH}/bin/python"
CELERY_EXE="${VENV_PATH}/bin/celery"

export PYTHONPATH="${SRC_DIR}:${PYTHONPATH:-}"

if [[ ! -x "$PYTHON_EXE" ]] || [[ ! -x "$CELERY_EXE" ]]; then
    echo "Ошибка: Python или Celery не найдены в venv" >&2
    exit 1
fi

# -------------------- Конфигурация --------------------
CPUSET_CPUS="8,10"
CPUSET_MEMS="0"
CELERY_APP="celery_app"
PYTHON_SCRIPT="${SRC_DIR}/main.py"
WORKER_LOG_PREFIX="${SCRIPT_DIR}/worker_profiled"
PERF_DATA_DIR="${SCRIPT_DIR}/perf_data"
mkdir -p "$PERF_DATA_DIR"

WORKER_PID=""
CGROUP_NAME=""
PERF_FORK_PID=""
PERF_CPU_PID=""
PERF_STAT_PID=""

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

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

remove_cgroup() {
    local cg_name="$1"
    if [[ -z "$cg_name" || ! -d "/sys/fs/cgroup/$cg_name" ]]; then
        return
    fi
    echo -e "${YELLOW}Удаляем cgroup $cg_name...${NC}"
    if [[ -f "/sys/fs/cgroup/$cg_name/cgroup.procs" ]]; then
        cat "/sys/fs/cgroup/$cg_name/cgroup.procs" 2>/dev/null | \
            sudo tee /sys/fs/cgroup/cgroup.procs >/dev/null || true
    fi
    sudo rmdir "/sys/fs/cgroup/$cg_name" 2>/dev/null || true
}

cleanup() {
    echo -e "${YELLOW}Очистка...${NC}"
    sudo kill -INT "$PERF_FORK_PID" "$PERF_CPU_PID" "$PERF_STAT_PID" 2>/dev/null || true
    stop_worker
    [[ -n "$CGROUP_NAME" ]] && remove_cgroup "$CGROUP_NAME"
}

# -------------------- Проверка окружения --------------------
if ! mount | grep -q "cgroup2 on /sys/fs/cgroup"; then
    error_exit "cgroups v2 не смонтированы."
fi
if [[ ! -f "/sys/fs/cgroup/cgroup.subtree_control" ]]; then
    error_exit "Нет cgroup.subtree_control"
fi
if ! grep -qw "cpuset" /sys/fs/cgroup/cgroup.subtree_control; then
    error_exit "cpuset не включён в subtree_control корневой cgroup. Выполните: echo '+cpuset' | sudo tee /sys/fs/cgroup/cgroup.subtree_control"
fi
for cmd in sudo tee pgrep; do
    if ! command -v "$cmd" &>/dev/null; then
        error_exit "Команда $cmd не найдена."
    fi
done
sudo -v

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
    echo "Тип очереди (v=накопительная, s=по расписанию(раз в 10 сек):"
    read -r QUEUE_TYPE
    echo "Максимум процессов (2, 4, 8):"
    read -r MAX_PROC
fi

if [[ ! "$WORKER_TYPE" =~ ^[aceg]$ ]]; then
    error_exit "Неверный тип worker'а. Допустимо: a, c, e, g"
fi
if [[ ! "$QUEUE_TYPE" =~ ^[vs]$ ]]; then
    error_exit "Неверный тип очереди. Допустимо: v, s"
fi
if [[ ! "$MAX_PROC" =~ ^(2|4|8)$ ]]; then
    error_exit "Неверное число процессов. Допустимо: 2, 4, 8"
fi

MAX_PROC_NUM=$((10#$MAX_PROC))
if [[ "$WORKER_TYPE" == "a" ]]; then
    MIN_PROC=0  # $((MAX_PROC_NUM / 2))  ставим нуль для более яркого профилирования
    PREFORK_ARG="--autoscale=$MAX_PROC_NUM,$MIN_PROC"
elif [[ "$WORKER_TYPE" == "e" ]]; then
    PREFORK_ARG="-P eventlet --concurrency=$MAX_PROC_NUM"
    CELERY_POOL="eventlet"
    export CELERY_POOL
elif [[ "$WORKER_TYPE" == "g" ]]; then
    PREFORK_ARG="-P gevent --concurrency=$MAX_PROC_NUM"
    CELERY_POOL="gevent"
    export CELERY_POOL
else
    PREFORK_ARG="--concurrency=$MAX_PROC_NUM"
fi


# Создание cgroup
CGROUP_NAME="celery_prof_$$_${WORKER_TYPE}_${QUEUE_TYPE}_${MAX_PROC}"
echo -e "${GREEN}Создаём cgroup $CGROUP_NAME на ядрах $CPUSET_CPUS...${NC}"
sudo mkdir -p "/sys/fs/cgroup/$CGROUP_NAME"
if [[ ! -f "/sys/fs/cgroup/$CGROUP_NAME/cpuset.cpus" ]]; then
    error_exit "cpuset не доступен в $CGROUP_NAME – проверьте subtree_control родительской группы"
fi
echo "$CPUSET_CPUS" | sudo tee "/sys/fs/cgroup/$CGROUP_NAME/cpuset.cpus" >/dev/null
echo "$CPUSET_MEMS"  | sudo tee "/sys/fs/cgroup/$CGROUP_NAME/cpuset.mems" >/dev/null
echo -e "${GREEN}Cgroup создана:${NC}"
echo "  cpuset.cpus: $(cat /sys/fs/cgroup/$CGROUP_NAME/cpuset.cpus)"
echo "  cpuset.mems: $(cat /sys/fs/cgroup/$CGROUP_NAME/cpuset.mems)"
trap cleanup EXIT INT TERM

# 1. fork/exit record
PERF_FORK_FILE="${PERF_DATA_DIR}/fork_${WORKER_TYPE}_${QUEUE_TYPE}_${MAX_PROC}.data"
sudo perf record -e sched:sched_process_fork,sched:sched_process_exit -G "$CGROUP_NAME" -a -o "$PERF_FORK_FILE" -- sleep 300 &
PERF_FORK_PID=$!

# 2. CPU sampling (используем cycles, которые точно есть)
PERF_CPU_FILE="${PERF_DATA_DIR}/cpu_${WORKER_TYPE}_${QUEUE_TYPE}_${MAX_PROC}.data"
sudo perf record -e cycles -G "$CGROUP_NAME" -a -F 99 --call-graph dwarf -o "$PERF_CPU_FILE" -- sleep 300 &
PERF_CPU_PID=$!

# 3. perf stat (переносим -G перед -o, события через -e)
PERF_STAT_FILE="${PERF_DATA_DIR}/stat_${WORKER_TYPE}_${QUEUE_TYPE}_${MAX_PROC}.txt"
sudo perf stat -e context-switches,cpu-migrations,page-faults,cycles,instructions -G "$CGROUP_NAME" -a -I 1000 -o "$PERF_STAT_FILE" -- sleep 300 &
PERF_STAT_PID=$!

sleep 1

# Worker
WORKER_LOG="${WORKER_LOG_PREFIX}_${WORKER_TYPE}_${QUEUE_TYPE}_${MAX_PROC}.log"
echo -e "${GREEN}Запускаем Celery worker...${NC}"

bash -c '
        cgname="$1"; shift
        echo $$ | sudo tee "/sys/fs/cgroup/$cgname/cgroup.procs" >/dev/null || {
            echo "Ошибка: не удалось записать PID в cgroup $cgname" >&2
            exit 1
        }
        exec "$@"
    ' _ "$CGROUP_NAME" "$CELERY_EXE" -A "$CELERY_APP" worker --loglevel=INFO "$PREFORK_ARG" > "$WORKER_LOG" 2>&1 &
    WORKER_PID=$!

sleep 0.5
if ! kill -0 "$WORKER_PID" 2>/dev/null; then
    echo -e "${RED}Worker упал при старте. Последние строки лога:${NC}"
    tail -20 "$WORKER_LOG"
    error_exit "Worker не запустился."
fi

cg_path=$(awk -F: '/0::/ {print $3}' "/proc/$WORKER_PID/cgroup" 2>/dev/null || true)
if [[ "$cg_path" != "/$CGROUP_NAME" ]]; then
    echo -e "${RED}Не удалось поместить worker в cgroup $CGROUP_NAME (текущий: $cg_path)${NC}"
    error_exit "Ошибка привязки к cgroup"
fi

# Нагрузка
echo -e "${GREEN}Запуск main.py (нагрузка)${NC}"
"$PYTHON_EXE" "$PYTHON_SCRIPT" "$WORKER_TYPE" "$MAX_PROC" "$QUEUE_TYPE" 1

echo -e "${GREEN}Эксперимент завершён, останавливаем perf...${NC}"
cleanup
echo -e "${GREEN}Результаты:${NC}"
echo "  - fork/exit events: $PERF_FORK_FILE"
echo "  - CPU sampling:     $PERF_CPU_FILE"
echo "  - stat (секундно):  $PERF_STAT_FILE"
echo "  - лог worker:       $WORKER_LOG"