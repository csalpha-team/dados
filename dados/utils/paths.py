"""Filesystem path helpers for dataset pipelines.

Convention (see REFACTORING.md §4.1): every pipeline writes temp data under
``tmp_data/<dataset_id>/{input,output}/`` at the repo root. Override the root
via the ``TMP_DATA_DIR`` env var.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Literal

TmpKind = Literal["input", "output"]


def tmp_root() -> Path:
    return Path(os.getenv("TMP_DATA_DIR", "tmp_data"))


def tmp_dir(dataset_id: str, kind: TmpKind) -> Path:
    """Return (and create) ``tmp_data/<dataset_id>/<kind>/``.

    ``kind`` must be ``"input"`` or ``"output"``.
    """
    if kind not in ("input", "output"):
        raise ValueError(f"kind must be 'input' or 'output', got {kind!r}")
    path = tmp_root() / dataset_id / kind
    path.mkdir(parents=True, exist_ok=True)
    return path
