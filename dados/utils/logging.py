"""Shared loguru logger factory for all dataset flows.

Usage:
    from dados.utils.logging import get_logger
    log = get_logger(dataset_id="al_ibge_ppm", zone="raw")
    log.info("flow.start")
    log.info("extract.done", rows=len(df))

Configuration happens once on first import: stdout sink + rotating file sink
under ``logs/<zone>/<dataset_id>.log``. Level is controlled by ``LOG_LEVEL``
(default ``INFO``).
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

from loguru import logger

_LOG_ROOT = Path(os.getenv("LOG_DIR", "logs"))
_LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
_FORMAT = (
    "{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | "
    "{extra[zone]}.{extra[dataset_id]} | {message} | {extra}"
)

_configured = False
_file_sinks: set[tuple[str, str]] = set()


def _configure_stdout_once() -> None:
    global _configured
    if _configured:
        return
    logger.remove()
    logger.add(
        sys.stdout,
        level=_LOG_LEVEL,
        format=_FORMAT,
        enqueue=False,
        backtrace=False,
        diagnose=False,
    )
    _configured = True


def _add_file_sink(zone: str, dataset_id: str) -> None:
    key = (zone, dataset_id)
    if key in _file_sinks:
        return
    log_path = _LOG_ROOT / zone / f"{dataset_id}.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logger.add(
        str(log_path),
        level=_LOG_LEVEL,
        format=_FORMAT,
        rotation="10 MB",
        retention=5,
        enqueue=False,
        filter=lambda r, z=zone, d=dataset_id: (
            r["extra"].get("zone") == z and r["extra"].get("dataset_id") == d
        ),
    )
    _file_sinks.add(key)


def get_logger(dataset_id: str, zone: str):
    """Return a loguru logger bound to ``dataset_id`` and ``zone``.

    Standard event names (use verbatim): ``flow.start``, ``flow.end``,
    ``extract.done``, ``validate.done``, ``transform.done``, ``load.done``,
    and ``*.error`` on failures.
    """
    _configure_stdout_once()
    _add_file_sink(zone=zone, dataset_id=dataset_id)
    return logger.bind(dataset_id=dataset_id, zone=zone)
