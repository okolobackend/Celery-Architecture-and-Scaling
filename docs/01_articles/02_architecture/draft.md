*Mother, in our pipes there is no water.
Yes, my darling daughter.*

файловые дескрипторы
sudo strace -p 370190 -e trace=read,write -y -s 100
lsof -p 370190 | grep pipe

/proc/[pid]/fd/
Список открытых файловых дескрипторов со ссылками. Вы увидите:
-- pipe:[номер] — пайпы между главным процессом и воркерами.
-- socket:[номер] — соединения с брокером (RabbitMQ/Redis).
Сравнивая FD главного процесса и воркеров, можно визуализировать каналы обмена.
