"""Helpers de paralelismo para os flows grandes (PAM, PEVS, PPM).

Duas etapas dominam o custo desses datasets e eram feitas em um único passo
sobre o dataframe inteiro:

* **validação pydantic** — ``[Model(**r) for r in df.to_dict("records")]``
  materializa a tabela toda como lista de dicionários antes de validar. Em
  tabelas de milhões de linhas isso sozinho consome vários GB.
* **leitura/parse dos JSON por município** — 773 arquivos lidos em série.

Aqui as duas viram trabalho por fatia, distribuído em threads. Threads (e não
processos) porque o núcleo do pydantic v2 é Rust e libera a GIL, e porque a
leitura de arquivo é I/O — sem o custo de copiar o dataframe para cada processo.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable, Iterable, List, Sequence, TypeVar

import pandas as pd

T = TypeVar("T")
R = TypeVar("R")

DEFAULT_MAX_WORKERS = 4
DEFAULT_CHUNK_SIZE = 50_000


def parallel_map(
    func: Callable[[T], R],
    items: Sequence[T],
    *,
    max_workers: int = DEFAULT_MAX_WORKERS,
    ordered: bool = True,
) -> List[R]:
    """Aplica ``func`` a cada item em threads.

    Com ``ordered=True`` o resultado sai na ordem da entrada, o que importa
    quando as partes são concatenadas em um dataframe determinístico.
    """
    if not items:
        return []
    if max_workers <= 1 or len(items) == 1:
        return [func(item) for item in items]

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        if ordered:
            return list(executor.map(func, items))

        futures = [executor.submit(func, item) for item in items]
        return [future.result() for future in as_completed(futures)]


def iter_chunks(
    df: pd.DataFrame, chunk_size: int = DEFAULT_CHUNK_SIZE
) -> Iterable[pd.DataFrame]:
    """Fatia um dataframe sem copiar a tabela inteira."""
    for start in range(0, len(df), chunk_size):
        yield df.iloc[start : start + chunk_size]


def validate_rows(
    df: pd.DataFrame,
    model: Any,
    *,
    max_workers: int = DEFAULT_MAX_WORKERS,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> None:
    """Valida todas as linhas contra o schema pydantic, por fatia e em threads.

    Levanta a primeira exceção encontrada, como a validação linha a linha
    original. O pico de memória fica em ``max_workers × chunk_size`` registros
    em vez do dataframe inteiro convertido para dicionários.
    """
    if df.empty:
        return

    chunks = list(iter_chunks(df, chunk_size))

    def _validate(chunk: pd.DataFrame) -> None:
        for registro in chunk.to_dict("records"):
            model(**registro)

    parallel_map(_validate, chunks, max_workers=max_workers)
