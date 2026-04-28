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
CPUSET_CPUS="8,10"         # первые потоки 5-го и 6-го физического ядра
CPUSET_MEMS="0"            # единственный NUMA-узел
CELERY_APP="celery_app"
PYTHON_SCRIPT="${SRC_DIR}/main.py"
WORKER_LOG_DIR="${SCRIPT_DIR}/worker_logs"
WORKER_LOG_PREFIX="${WORKER_LOG_DIR}/worker"
SLEEP_BETWEEN_ITER=20
ITERATIONS_PER_COMBO=20

mkdir -p "$WORKER_LOG_DIR"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Глобальные переменные для текущего запуска (будут перезаписываться в каждой итерации)
WORKER_PID=""
CGROUP_NAME=""
CREATED_CGROUPS=()   # для финальной очистки

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

cleanup_current() {
    echo -e "${YELLOW}Очистка текущей итерации...${NC}"
    stop_worker
    if [[ -n "$CGROUP_NAME" ]]; then
        remove_cgroup "$CGROUP_NAME"
        CGROUP_NAME=""
    fi
}

final_cleanup() {
    echo -e "${YELLOW}Финальная очистка...${NC}"
    stop_worker
    for cg in "${CREATED_CGROUPS[@]}"; do
        remove_cgroup "$cg"
    done
}

start_worker() {
    local iteration="$1"
    local cg_name="$2"
    local prefork_arg="$3"
    local worker_log="${WORKER_LOG_PREFIX}_${cg_name}_${iteration}.log"

    echo -e "${GREEN}Запускаем Celery worker (лог: $worker_log)${NC}"

    bash -c '
        cgname="$1"; shift
        echo $$ | sudo tee "/sys/fs/cgroup/$cgname/cgroup.procs" >/dev/null || {
            echo "Ошибка: не удалось записать PID в cgroup $cgname" >&2
            exit 1
        }
        exec "$@"
    ' _ "$cg_name" "$CELERY_EXE" -A "$CELERY_APP" worker --loglevel=INFO "$prefork_arg" > "$worker_log" 2>&1 &
    WORKER_PID=$!

    sleep 0.5
    if ! kill -0 "$WORKER_PID" 2>/dev/null; then
        echo -e "${RED}Worker упал при старте. Последние строки лога:${NC}"
        tail -20 "$worker_log"
        error_exit "Worker не запустился."
    fi

    local cg_path
    cg_path=$(awk -F: '/0::/ {print $3}' "/proc/$WORKER_PID/cgroup" 2>/dev/null || true)
    if [[ "$cg_path" != "/$cg_name" ]]; then
        echo -e "${RED}Не удалось поместить worker в cgroup $cg_name (текущий: $cg_path)${NC}"
        error_exit "Ошибка привязки к cgroup"
    fi

    echo -e "${GREEN}Worker PID $WORKER_PID в cgroup $cg_name${NC}"
    local mask
    mask=$(taskset -p "$WORKER_PID" 2>/dev/null | awk '{print $NF}')
    echo -e "${GREEN}Affinity-маска главного процесса: $mask${NC}"
}

create_cgroup() {
    local cg_name="$1"
    echo -e "${GREEN}Создаём cgroup '$cg_name' cpuset.cpus=$CPUSET_CPUS...${NC}"
    sudo mkdir -p "/sys/fs/cgroup/$cg_name"
    if [[ ! -f "/sys/fs/cgroup/$cg_name/cpuset.cpus" ]]; then
        error_exit "cpuset не доступен в $cg_name – проверьте subtree_control родительской группы"
    fi
    echo "$CPUSET_CPUS" | sudo tee "/sys/fs/cgroup/$cg_name/cpuset.cpus" >/dev/null
    echo "$CPUSET_MEMS"  | sudo tee "/sys/fs/cgroup/$cg_name/cpuset.mems" >/dev/null
    echo -e "${GREEN}Cgroup создана:${NC}"
    echo "  cpuset.cpus: $(cat /sys/fs/cgroup/$cg_name/cpuset.cpus)"
    echo "  cpuset.mems: $(cat /sys/fs/cgroup/$cg_name/cpuset.mems)"
}

# Выполнение одной итерации для заданной комбинации и номера
run_one_iteration() {
    local wt="$1"
    local qt="$2"
    local proc="$3"
    local iteration="$4"

    local proc_num=$((10#$proc))
    local prefork_arg
    if [[ "$wt" == "a" ]]; then
        local min_proc=$((proc_num / 2))
        prefork_arg="--autoscale=$proc_num,$min_proc"
    else
        prefork_arg="--concurrency=$proc_num"
    fi

    local combo_name="${wt}_${qt}_${proc}"
    local cg_name="celery_exp_$$_${combo_name}_iter${iteration}"
    CREATED_CGROUPS+=("$cg_name")   # для финальной очистки на случай аварийного выхода

    echo -e "${YELLOW}========== Итерация: $combo_name : $iteration ==========${NC}"
    echo "Параметры: worker=$wt, queue=$qt, max_proc=$proc, prefork='$prefork_arg'"

    create_cgroup "$cg_name"
    CGROUP_NAME="$cg_name"

    # Локальный trap для очистки этой итерации при прерывании внутри
    trap cleanup_current EXIT INT TERM

    start_worker "$iteration" "$cg_name" "$prefork_arg"
    $PYTHON_EXE "$PYTHON_SCRIPT" "$wt" "$proc" "$qt" "$iteration"
    sleep "$SLEEP_BETWEEN_ITER"

    cleanup_current
    trap - EXIT INT TERM

    # После каждого замера сбрасываем кэш (как и раньше)
    if command -v sudo &>/dev/null; then
        echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null 2>&1 || true
    fi
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

# -------------------- Генерация всех заданий (комбинация + номер итерации) --------------------
WORKER_TYPES=("a" "c")
QUEUE_TYPES=("v" "s")
PROC_COUNTS=(2 4 8)

declare -a ALL_TASKS=()

for wt in "${WORKER_TYPES[@]}"; do
    for qt in "${QUEUE_TYPES[@]}"; do
        for pc in "${PROC_COUNTS[@]}"; do
            for iter in $(seq 1 $ITERATIONS_PER_COMBO); do
                ALL_TASKS+=("$wt $qt $pc $iter")
            done
        done
    done
done

echo -e "${GREEN}Всего заданий: ${#ALL_TASKS[@]} (${ITERATIONS_PER_COMBO} итераций × 12 комбинаций)${NC}"

# Перемешиваем все задания
if command -v shuf >/dev/null; then
    mapfile -t ALL_TASKS < <(printf "%s\n" "${ALL_TASKS[@]}" | shuf)
else
    mapfile -t ALL_TASKS < <(printf "%s\n" "${ALL_TASKS[@]}" | sort -R)
fi

echo "Порядок выполнения (первые 10):"
for ((i=0; i<10 && i<${#ALL_TASKS[@]}; i++)); do
    echo "  ${ALL_TASKS[$i]}"
done
if [[ ${#ALL_TASKS[@]} -gt 10 ]]; then
    echo "  ..."
fi

# Глобальный trap на случай аварийного завершения
trap final_cleanup EXIT INT TERM

# -------------------- Основной цикл --------------------
START_TIME=$(date +%s)
TOTAL=${#ALL_TASKS[@]}
CURRENT=0

for task in "${ALL_TASKS[@]}"; do
    read -r wt qt proc iter <<< "$task"
    CURRENT=$((CURRENT + 1))
    echo -e "${GREEN}[$CURRENT/$TOTAL]${NC} Запуск: $wt $qt $proc итерация $iter"
    run_one_iteration "$wt" "$qt" "$proc" "$iter"
done

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))
echo -e "${GREEN}Все итерации выполнены за $((DURATION / 3600)) ч $(( (DURATION % 3600) / 60 )) мин.${NC}"