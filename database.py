"""
Persistência local dos LPCOs conhecidos (submetidos pela Hevile no portal).
Usado para filtrar eventos de webhook — só notifica processos nossos.
"""

import logging
import sqlite3
from pathlib import Path

from siscomex_client import LpcoRecord

logger = logging.getLogger(__name__)

DB_PATH = Path("lpco_monitor.db")


def init_db() -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS lpcos_conhecidos (
                numero        TEXT PRIMARY KEY,
                codigo_modelo TEXT,
                situacao      TEXT,
                data_sync     TEXT DEFAULT (datetime('now'))
            )
        """)
        conn.commit()
    logger.info("Banco de dados inicializado em %s", DB_PATH)


def salvar_lpcos(lpcos: list[LpcoRecord]) -> int:
    """Upsert de uma lista de LPCOs. Retorna quantos foram inseridos/atualizados."""
    if not lpcos:
        return 0
    with sqlite3.connect(DB_PATH) as conn:
        conn.executemany(
            """
            INSERT INTO lpcos_conhecidos (numero, codigo_modelo, situacao, data_sync)
            VALUES (?, ?, ?, datetime('now'))
            ON CONFLICT(numero) DO UPDATE SET
                situacao  = excluded.situacao,
                data_sync = excluded.data_sync
            """,
            [(lp.numero, lp.tipo, lp.situacao) for lp in lpcos],
        )
        conn.commit()
    return len(lpcos)


def lpco_conhecido(numero: str) -> bool:
    """Retorna True se o número de LPCO foi submetido pela Hevile."""
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            "SELECT 1 FROM lpcos_conhecidos WHERE numero = ?", (numero,)
        ).fetchone()
    return row is not None


def total_lpcos() -> int:
    with sqlite3.connect(DB_PATH) as conn:
        return conn.execute("SELECT COUNT(*) FROM lpcos_conhecidos").fetchone()[0]
