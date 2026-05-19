"""
Persistência local dos LPCOs conhecidos (submetidos pela Hevile no portal).
Usado para filtrar eventos de webhook — só notifica processos nossos.
"""

import logging
import sqlite3
from pathlib import Path

logger = logging.getLogger(__name__)

DB_PATH = Path("lpco_monitor.db")


def init_db() -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS lpcos_conhecidos (
                numero    TEXT PRIMARY KEY,
                origem    TEXT DEFAULT 'manual',
                data_sync TEXT DEFAULT (datetime('now'))
            )
        """)
        conn.commit()
    logger.info("Banco de dados inicializado em %s", DB_PATH)


def registrar_lpco(numero: str) -> bool:
    """Registra um LPCO como sendo da Hevile. Retorna True se foi inserido (novo)."""
    numero = numero.strip().upper()
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.execute(
            "INSERT OR IGNORE INTO lpcos_conhecidos (numero) VALUES (?)", (numero,)
        )
        conn.commit()
    inserido = cur.rowcount > 0
    if inserido:
        logger.info("LPCO %s registrado no banco.", numero)
    return inserido


def lpco_conhecido(numero: str) -> bool:
    """Retorna True se o número de LPCO está registrado como sendo da Hevile."""
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            "SELECT 1 FROM lpcos_conhecidos WHERE numero = ?", (numero.strip().upper(),)
        ).fetchone()
    return row is not None


def total_lpcos() -> int:
    with sqlite3.connect(DB_PATH) as conn:
        return conn.execute("SELECT COUNT(*) FROM lpcos_conhecidos").fetchone()[0]
