perf record -e cpu-cycles,instructions,context-switches,cpu-migrations -G /sys/fs/cgroup/имя_cgroup -o "file_output.data" -- sleep infinity
