import asyncio
import json
import os
from datetime import datetime

import aiofiles
import aiohttp


class QueueMonitor:

    __slots__ = ['api_url', 'auth', 'idle_threshold', 'output_dir', 'history',
                 'active_periods', '_active_start', '_active_ack_start']

    def __init__(self, api_url="http://localhost:15672/api/queues/%2F/celery", auth=('celery_demo', 'celery_demo'), idle_threshold=3, output_dir=None):
        self.api_url = api_url
        self.auth = aiohttp.BasicAuth(*auth)
        self.idle_threshold = idle_threshold
        self.output_dir = output_dir
        self.history = []
        self.active_periods = []
        self._active_start = None
        self._active_ack_start = None

    async def purge_queue(self):
        """
        Принудительно очищаем очередь для следующего запуска
        """
        purge_url = self.api_url + '/' + 'contents'
        try:
            async with aiohttp.ClientSession() as session:
                async with session.delete(purge_url, auth=self.auth) as response:
                    if response.status == 204:
                        print(f"Очередь очищена.")
                    else:
                        error_text = await response.text()
                        print(f"Ошибка при очистке очереди: Статус {response.status}, Ответ: {error_text}")
        except aiohttp.ClientConnectorError:
            print(f"Не удалось подключиться к {purge_url}.")
        except Exception as e:
            print(f"Непредвиденная ошибка: {e}")

    async def _fetch_stats(self, session):
        ts = datetime.now().timestamp()
        try:
            async with session.get(self.api_url, auth=self.auth) as resp:
                data = await resp.json()

                return {
                    'timestamp': ts,
                    'messages': data.get('messages', 0),
                    'ack': data.get('message_stats', {}).get('ack', 0)
                }

        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            return {
                'timestamp': ts,
                'error': str(e),
                'messages': None,
                'ack': None
            }


    async def update_stats(self):
        async with aiohttp.ClientSession() as session:
            stats = await self._fetch_stats(session)
            self.history.append(stats)
            try:
                now = stats['timestamp']
                ack = stats['ack']
            except KeyError:
                return

            if self._active_start is None:
                self._active_start = now
                self._active_ack_start = ack

    def _close_active_period(self):
        end_time = datetime.now().timestamp()
        duration = end_time - self._active_start
        if duration <= 0:
            return

        last_ack = self.history[-1]['ack'] if self.history else self._active_ack_start
        tasks_completed = last_ack - self._active_ack_start
        throughput = tasks_completed / duration if duration > 0 else 0
        self.active_periods.append({
            'start': self._active_start,
            'end': end_time,
            'duration_sec': duration,
            'tasks_completed': tasks_completed,
            'throughput_tps': throughput
        })

    async def write_to_file(self):
        self._close_active_period()
        filepath = os.path.join(self.output_dir, "queue_throughput.log")
        data = {
            "active_periods": self.active_periods,
            "history": self.history,
        }
        async with aiofiles.open(filepath, "w", encoding='utf-8') as f:
            await f.write(json.dumps(data, indent=4, ensure_ascii=False))
