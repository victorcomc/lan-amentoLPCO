"""
Persistência local dos LPCOs conhecidos (submetidos pela Hevile no portal).
Usado para filtrar eventos de webhook — só notifica processos nossos.
"""

import logging
import sqlite3
from pathlib import Path
from typing import Any

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
        conn.execute("""
            CREATE TABLE IF NOT EXISTS eventos (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                data_evento     TEXT,
                data_recebido   TEXT DEFAULT (datetime('now')),
                numero_lpco     TEXT,
                codigo_modelo   TEXT,
                tipo            TEXT,
                regiao          TEXT,
                destinatario_id TEXT,
                situacao_id     TEXT,
                situacao_desc   TEXT,
                justificativa   TEXT,
                cnpj_cpf        TEXT,
                payload_json    TEXT
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


def registrar_evento(dados: dict[str, Any]) -> None:
    """Persiste um evento de webhook recebido para fins de histórico e relatório."""
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            INSERT INTO eventos
                (data_evento, numero_lpco, codigo_modelo, tipo, regiao,
                 destinatario_id, situacao_id, situacao_desc, justificativa,
                 cnpj_cpf, payload_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                dados.get("data_evento", ""),
                dados.get("numero_lpco", ""),
                dados.get("codigo_modelo", ""),
                dados.get("tipo", ""),
                dados.get("regiao", ""),
                dados.get("destinatario_id", ""),
                dados.get("situacao_id", ""),
                dados.get("situacao_desc", ""),
                dados.get("justificativa", ""),
                dados.get("cnpj_cpf", ""),
                dados.get("payload_json", ""),
            ),
        )
        conn.commit()


def listar_eventos(data_inicio: str = "", data_fim: str = "") -> list[dict]:
    """
    Retorna eventos ordenados por data. Filtros opcionais: data_inicio/data_fim (YYYY-MM-DD).
    Quando data_fim é fornecida inclui o dia inteiro (até 23:59:59).
    """
    query = "SELECT * FROM eventos WHERE 1=1"
    params: list[str] = []
    if data_inicio:
        query += " AND data_evento >= ?"
        params.append(data_inicio)
    if data_fim:
        query += " AND data_evento <= ?"
        params.append(data_fim + " 23:59:59")
    query += " ORDER BY data_evento ASC"
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(query, params).fetchall()
    return [dict(row) for row in rows]
