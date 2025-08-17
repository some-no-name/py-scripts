import logging
import re
import threading
import time
from queue import Queue, Empty
from typing import Callable, Optional

from telegram import Bot
from telegram.error import RetryAfter, TimedOut, NetworkError

DEFAULT_FLOOD_WAIT = 1.0
DEFAULT_MAX_TRIES = 7

def _extract_wait_seconds(exc: Exception) -> Optional[int]:
    """
    Fallback parser for non-RetryAfter errors that include 'retry after N'.
    """
    m = re.search(r"retry after (\d+)", str(exc).lower())
    return int(m.group(1)) + 1 if m else None


class TelegramQueue:
    """
    A single-threaded FIFO sender for Telegram messages/documents.

    - Guarantees strict order of sends
    - Retries with backoff on flood control / network errors
    - Provides join() to wait until all queued items are sent
    - Logs sent message count and remaining queue size
    """

    def __init__(
        self,
        token: str,
        chat_id: str,
        *,
        base_delay: float = DEFAULT_FLOOD_WAIT,
        max_tries: int = DEFAULT_MAX_TRIES,
        name: str = "TelegramQueue",
        start_worker: bool = True,
    ) -> None:
        self._bot = Bot(token=token)
        self._chat_id = chat_id
        self._queue: "Queue[Callable[[Bot], None]]" = Queue()
        self._stop_event = threading.Event()
        self._base_delay = float(base_delay)
        self._max_tries = int(max_tries)
        self._sent_count = 0
        self._lock = threading.Lock()
        self._thread = threading.Thread(target=self._worker, name=name, daemon=True)
        if start_worker:
            self._thread.start()

    # ---------- Public API ----------

    def send_message(self, text: str, **kwargs) -> None:
        """
        Queue a text message. Extra keyword args are passed to bot.send_message.
        """
        def task(bot: Bot) -> None:
            bot.send_message(chat_id=self._chat_id, text=text, **kwargs)

        self._queue.put(task)

    def send_document(self, path: str, *, caption: Optional[str] = None, **kwargs) -> None:
        """
        Queue a document by file path.
        """
        def task(bot: Bot) -> None:
            with open(path, "rb") as f:
                bot.send_document(chat_id=self._chat_id, document=f, caption=caption, **kwargs)

        self._queue.put(task)

    def join(self, timeout: Optional[float] = None) -> None:
        """Block until all queued tasks are processed."""
        self._queue.join()
        if timeout is not None:
            self._thread.join(timeout)

    def close(self) -> None:
        """Stop worker after processing current queue."""
        self._stop_event.set()
        self._queue.put(lambda _b: None)  # wake worker if idle
        self._thread.join()

    def __enter__(self) -> "TelegramQueue":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.join()
        self.close()

    # ---------- Internal ----------

    def _worker(self) -> None:
        while not self._stop_event.is_set():
            try:
                task = self._queue.get(timeout=0.25)
            except Empty:
                continue

            tries = 0
            delay = self._base_delay

            while tries < self._max_tries:
                try:
                    task(self._bot)
                    with self._lock:
                        self._sent_count += 1
                        sent_id = self._sent_count
                    remaining = self._queue.qsize()
                    logging.info(
                        "✅ Message sent. Incremental ID: %d, Queue left: %d",
                        sent_id, remaining
                    )
                    break
                except RetryAfter as e:
                    wait_s = int(getattr(e, "retry_after", 0)) + 1
                    logging.warning("Telegram RetryAfter: sleeping %s s", wait_s)
                    time.sleep(wait_s)
                except (TimedOut, NetworkError) as e:
                    tries += 1
                    logging.warning("Telegram network issue (%s). Retry #%s in %.1fs",
                                    e.__class__.__name__, tries, delay)
                    time.sleep(delay)
                    delay = min(delay * 2, 60.0)
                except Exception as e:
                    wait = _extract_wait_seconds(e)
                    if wait is not None:
                        logging.warning("Telegram flood hint: sleeping %s s", wait)
                        time.sleep(wait)
                    else:
                        logging.error("Telegram send error (no retry): %s", e)
                        break

            self._queue.task_done()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    TELEGRAM_BOT_TOKEN = None
    TELEGRAM_CHANNEL_ID = None

    tq = TelegramQueue(TELEGRAM_BOT_TOKEN, TELEGRAM_CHANNEL_ID, base_delay=1.0, max_tries=7)
    print('-111')
    tq.send_message("Start")
    print('-222')
    for i in range(30):
        tq.send_message(f"Message #{i+1}")
    # tq.send_document("bigfile.zip")
    print('-333')

    # Make sure we wait before the script exits:
    tq.join()   # wait until the queue is empty
    tq.close()  # stop worker thread cleanly

    print('-444')
