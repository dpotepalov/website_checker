import asyncio
import logging
import signal

from contextlib import asynccontextmanager


def _handle_termination(signame, loop):
    logging.error('received %s, stopping', signame)
    for task in asyncio.Task.all_tasks():
        task.cancel()


def setup_termination():
    loop = asyncio.get_running_loop()
    for signame in {'SIGINT', 'SIGTERM'}:
        loop.add_signal_handler(getattr(signal, signame), _handle_termination, signame, loop)


def setup_logging():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')


@asynccontextmanager
async def quit_if_cancelled():
    try:
        yield
    except asyncio.CancelledError:
        logging.error('cancelled, processing stopped')
