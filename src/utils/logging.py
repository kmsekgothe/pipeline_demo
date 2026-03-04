import logging
import sys
import time
from contextlib import contextmanager


@contextmanager
def log_step(logger, step_name: str):
    start = time.perf_counter()
    logger.info(f"{step_name} started")

    try:
        yield
        duration = time.perf_counter() - start
        logger.info(f"{step_name} completed in {duration:.2f}s")
    except Exception:
        duration = time.perf_counter() - start
        logger.exception(f"{step_name} failed after {duration:.2f}s")
        raise

def get_logger(name: str = "pipeline") -> logging.Logger:
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger  # prevent duplicate handlers

    logger.setLevel(logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)

    logger.addHandler(handler)

    return logger