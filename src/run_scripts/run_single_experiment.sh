#!/bin/bash
set -euo pipefail

EXPERIMENT_TIMESTAMP="${EXPERIMENT_TIMESTAMP:-$(date +"%Y-%m-%d_%H-%M-%S")}"
export EXPERIMENT_TIMESTAMP

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
USE_CPUSET="yes"          # "yes" – привязать к ядрам 0-1, "no" – без cpuset
CGROUP_NAME="celery_exp_$$"
CPU_QUOTA="200000"        # 200% от CPU_PERIOD → 2 ядра
CPU_PERIOD="100000"
CELERY_APP="celery_app"
PYTHON_SCRIPT="${SCRIPT_DIR}/main.py"
LOG_DIR_BASE="../docs/02_runtime"
WORKER_LOG_PREFIX="${SCRIPT_DIR}/worker"   # К логу будет добавляться номер итерации
SLEEP_BETWEEN=2
ITERATIONS_PER_COMBO=20

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
    local iteration="$1"
    local worker_log="${WORKER_LOG_PREFIX}_${iteration}.log"

    echo -e "${GREEN}Запускаем Celery worker (лог: $worker_log)${NC}"

    # Формируем опции cgroups для cgexec
    local CGEXEC_OPTS="-g cpu:/$CGROUP_NAME"
    if [[ "$USE_CPUSET" == "yes" ]]; then
        CGEXEC_OPTS="$CGEXEC_OPTS -g cpuset:/$CGROUP_NAME"
    fi

    # Запускаем worker через cgexec, чтобы все процессы (включая дочерние) попали в cgroup
    sudo cgexec $CGEXEC_OPTS \
        "$CELERY_EXE" -A "$CELERY_APP" worker --loglevel=INFO \
        "$PREFORK_ARG" > "$worker_log" 2>&1 &
    WORKER_PID=$!

    sleep 2
    if ! kill -0 "$WORKER_PID" 2>/dev/null; then
        echo -e "${RED}Worker упал при старте. Последние строки лога:${NC}"
        tail -20 "$worker_log"
        error_exit "Worker не запустился."
    fi
    echo -e "${GREEN}Worker запущен, PID: $WORKER_PID (внутри cgroup $CGROUP_NAME)${NC}"

    # Выводим информацию о cgroup (для верификации)
    echo -e "${GREEN}Проверяем настройки CPU для cgroup:${NC}"
    echo "  - Разрешённые ядра (taskset для main процесса): "
    taskset -cp "$WORKER_PID" 2>/dev/null | sed 's/^/    /' || echo "    Не удалось определить"
    echo -n "  - Эффективные ядра cpuset: "
    sudo cat "/sys/fs/cgroup/$CGROUP_NAME/cpuset.cpus.effective" 2>/dev/null || echo "cpuset не активен или нет прав"
    echo -n "  - CPU квота (cpu.max): "
    sudo cat "/sys/fs/cgroup/$CGROUP_NAME/cpu.max" 2>/dev/null || echo "не задана"
}

# -------------------- Проверка окружения --------------------
# 1. Проверка cgroups v2
if ! mount | grep -q "cgroup2 on /sys/fs/cgroup"; then
    error_exit "cgroups v2 не смонтированы."
fi

# 2. Проверка наличия cgexec
if ! command -v cgexec &>/dev/null; then
    error_exit "cgexec не найден. Установите cgroup-tools (например, apt install cgroup-tools)"
fi

# 3. Проверка прав sudo (первый вызов sudo потребует пароль, но не прервёт скрипт)
sudo -v || error_exit "Необходимы права sudo для управления cgroups"

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
    echo "Тип worker'а (a=autoscale, c=concurrency, e=eventlet, g=gevent):"
    read -r WORKER_TYPE
    echo "Тип очереди (v=накопительная, s=по расписанию(раз в 10 сек):"
    read -r QUEUE_TYPE
    echo "Максимум процессов (2, 4, 8):"
    read -r MAX_PROC
fi

if [[ ! "$WORKER_TYPE" =~ ^[aceg]$ ]]; then
    error_exit "Неверный тип worker'а. Допустимо: a, c"
fi
if [[ ! "$QUEUE_TYPE" =~ ^[vs]$ ]]; then
    error_exit "Неверный тип очереди. Допустимо: v, s"
fi
if [[ ! "$MAX_PROC" =~ ^(2|4|8)$ ]]; then
    error_exit "Неверное число процессов. Допустимо: 2, 4, 8"
fi

MAX_PROC_NUM=$((10#$MAX_PROC))
if [[ "$WORKER_TYPE" == "a" ]]; then
    MIN_PROC=$((MAX_PROC_NUM / 2))
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

# -------------------- Создание cgroup (правильный порядок) --------------------
echo -e "${GREEN}Создаём cgroup '$CGROUP_NAME' с ограничением в 2 ядра...${NC}"

# 1. Включаем контроллеры cpu и cpuset в родительской cgroup (корневой)
#    Это необходимо сделать до создания дочерней cgroup, иначе дочерняя не получит контроллеры.
if ! grep -q "cpu" /sys/fs/cgroup/cgroup.controllers 2>/dev/null; then
    error_exit "Контроллер cpu недоступен в корневой cgroup"
fi
if [[ "$USE_CPUSET" == "yes" ]] && ! grep -q "cpuset" /sys/fs/cgroup/cgroup.controllers 2>/dev/null; then
    error_exit "Контроллер cpuset недоступен в корневой cgroup"
fi

echo "+cpu" | sudo tee /sys/fs/cgroup/cgroup.subtree_control >/dev/null
if [[ "$USE_CPUSET" == "yes" ]]; then
    echo "+cpuset" | sudo tee /sys/fs/cgroup/cgroup.subtree_control >/dev/null
fi

# 2. Создаём дочернюю cgroup
sudo mkdir -p "/sys/fs/cgroup/$CGROUP_NAME"

# 3. Устанавливаем квоту CPU (2 ядра)
echo "$CPU_QUOTA $CPU_PERIOD" | sudo tee "/sys/fs/cgroup/$CGROUP_NAME/cpu.max" >/dev/null

# 4. Если нужна привязка к ядрам – задаём cpuset
if [[ "$USE_CPUSET" == "yes" ]]; then
    echo "9,10" | sudo tee "/sys/fs/cgroup/$CGROUP_NAME/cpuset.cpus" >/dev/null
    echo "0"   | sudo tee "/sys/fs/cgroup/$CGROUP_NAME/cpuset.mems" >/dev/null
fi

# Проверяем, что cgroup создана корректно
if [[ ! -d "/sys/fs/cgroup/$CGROUP_NAME" ]]; then
    error_exit "Не удалось создать cgroup $CGROUP_NAME"
fi

trap cleanup EXIT INT TERM

# -------------------- Цикл экспериментов --------------------
echo -e "${GREEN}Начинаем серию из 15 экспериментов. На каждой итерации worker перезапускается.${NC}"

for order in $(seq 1 $ITERATIONS_PER_COMBO); do
    echo -e "${YELLOW}--- Запуск $order из ${ITERATIONS_PER_COMBO} ---${NC}"

    start_worker "$order"

    # Запуск Python скрипта, который генерирует нагрузку и собирает метрики
    # (предполагается, что скрипт принимает те же аргументы и работает синхронно)
    $PYTHON_EXE "$PYTHON_SCRIPT" "$WORKER_TYPE" "$MAX_PROC" "$QUEUE_TYPE" "$order"

    sleep "$SLEEP_BETWEEN"

    stop_worker
done

echo -e "${GREEN}Все 15 экспериментов завершены. Результаты в $LOG_DIR_BASE/${NC}"
cleanup