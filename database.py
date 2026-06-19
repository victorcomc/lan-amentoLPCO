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

# CNPJs dos clientes da Hevile — populados automaticamente no init_db().
# Atualizar aqui quando houver novos clientes.
_CLIENTES_SE = [
    "48812543000130", "00605555000167", "53102491000148", "64654650000133",
    "26244457000135", "11591434000120", "55573936000101", "42135730000140",
    "48115574000131", "56026840000188", "08215522000112", "08215522000546",
    "08215522000627", "08215522000708", "08215522000970", "31276127000161",
    "15452593001176", "15531898000100", "41391017000102", "41391017000285",
    "41999541000152", "56420707000101",
]

_CLIENTES_NE = [
    "12786836000304", "38056418000436", "70092545408",    "43871437000103",
    "02035825000177", "10879115000151", "40338215000131", "40338215000301",
    "02968267000100", "68901040000148", "26332897000144", "33323324000110",
    "04749699403",    "96736350001242", "96736350000190", "20928862000196",
    "44273202000182", "07276194000282", "03068272000200", "19485654000180",
    "09215311000142", "03271313148",    "53111600000193", "11591434000120",
    "11591434000200", "04264905000120", "55573936000101", "27297671000112",
    "22915143000166", "18450755000153", "02851995000120", "08215522000546",
    "08215522000627", "08215522000708", "08215522000970", "08215522000112",
    "14419108000128", "11034952000142", "11034952000304", "18783557000101",
    "18693502000100", "12492143000147", "15452593001176", "08432692000159",
    "17247892000122", "28463606000182", "03338912000166", "23777347000140",
]


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
        # Tabela para pesquisa de mercado — todos os eventos DUE recebidos via webhook
        # (inclui DUEs de concorrentes / empresas que não são clientes da Hevile)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS dues_mercado (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                data_evento      TEXT,
                data_recebido    TEXT DEFAULT (datetime('now')),
                numero_due       TEXT,
                ruc              TEXT,
                tipo_evento      TEXT,
                descricao_evento TEXT,
                exportador_cnpj  TEXT,
                exportador_nome  TEXT,
                produto_ncm      TEXT,
                produto_desc     TEXT,
                peso_liquido_kg  REAL,
                peso_bruto_kg    REAL,
                valor_fob_usd    REAL,
                pais_destino     TEXT,
                porto_embarque   TEXT,
                embarcacao       TEXT,
                payload_json     TEXT,
                detalhe_json     TEXT
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_dues_numero     ON dues_mercado (numero_due)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_dues_exportador ON dues_mercado (exportador_cnpj)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_dues_data       ON dues_mercado (data_evento)")
        # CNPJs dos clientes da Hevile — usados para filtrar o relatório semanal
        conn.execute("""
            CREATE TABLE IF NOT EXISTS clientes_cnpj (
                cnpj       TEXT PRIMARY KEY,
                nome       TEXT DEFAULT '',
                ativo      INTEGER DEFAULT 1,
                data_sync  TEXT DEFAULT (datetime('now'))
            )
        """)
        conn.commit()

    # Auto-seed: garante que os CNPJs de clientes estão sempre populados,
    # mesmo após um deploy que recria o container (e apaga o banco).
    _seed_clientes_se_vazio()

    logger.info("Banco de dados inicializado em %s", DB_PATH)


def _seed_clientes_se_vazio() -> None:
    """Popula clientes_cnpj se a tabela estiver vazia (deploy limpo)."""
    with sqlite3.connect(DB_PATH) as conn:
        total = conn.execute("SELECT COUNT(*) FROM clientes_cnpj WHERE ativo=1").fetchone()[0]
        if total > 0:
            return
        todos = set(_CLIENTES_SE + _CLIENTES_NE)
        conn.executemany(
            "INSERT OR IGNORE INTO clientes_cnpj (cnpj, ativo) VALUES (?, 1)",
            [(c,) for c in todos],
        )
        conn.commit()
        logger.info("Auto-seed: %d CNPJs de clientes inseridos na tabela clientes_cnpj.", len(todos))


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


def listar_lpcos_conhecidos() -> list[str]:
    """Retorna todos os números de LPCO registrados no banco."""
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute("SELECT numero FROM lpcos_conhecidos").fetchall()
    return [row[0] for row in rows]


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


def registrar_due(dados: dict[str, Any]) -> int:
    """
    Persiste um evento DUE recebido via webhook para pesquisa de mercado.
    Retorna o id do registro inserido.
    """
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.execute(
            """
            INSERT INTO dues_mercado
                (data_evento, numero_due, ruc, tipo_evento, descricao_evento,
                 exportador_cnpj, exportador_nome, produto_ncm, produto_desc,
                 peso_liquido_kg, peso_bruto_kg, valor_fob_usd,
                 pais_destino, porto_embarque, embarcacao,
                 payload_json, detalhe_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                dados.get("data_evento", ""),
                dados.get("numero_due", ""),
                dados.get("ruc", ""),
                dados.get("tipo_evento", ""),
                dados.get("descricao_evento", ""),
                dados.get("exportador_cnpj", ""),
                dados.get("exportador_nome", ""),
                dados.get("produto_ncm", ""),
                dados.get("produto_desc", ""),
                dados.get("peso_liquido_kg"),
                dados.get("peso_bruto_kg"),
                dados.get("valor_fob_usd"),
                dados.get("pais_destino", ""),
                dados.get("porto_embarque", ""),
                dados.get("embarcacao", ""),
                dados.get("payload_json", ""),
                dados.get("detalhe_json", ""),
            ),
        )
        conn.commit()
        return cur.lastrowid or 0


def atualizar_due_detalhe(due_id: int, campos: dict[str, Any]) -> None:
    """Enriquece um registro DUE existente com dados obtidos da API de detalhe."""
    sets = ", ".join(f"{k} = ?" for k in campos if k != "id")
    vals = [v for k, v in campos.items() if k != "id"]
    vals.append(due_id)
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(f"UPDATE dues_mercado SET {sets} WHERE id = ?", vals)
        conn.commit()


def listar_dues(data_inicio: str = "", data_fim: str = "") -> list[dict]:
    """Retorna DUEs de mercado ordenadas por data."""
    query = "SELECT * FROM dues_mercado WHERE 1=1"
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


# ---------------------------------------------------------------------------
# CNPJs dos clientes — filtro do relatório semanal
# ---------------------------------------------------------------------------

def _normalizar_cnpj(cnpj: str) -> str:
    return "".join(c for c in cnpj if c.isdigit())


def registrar_cnpj_cliente(cnpj: str, nome: str = "") -> bool:
    """Registra CNPJ como cliente ativo. Retorna True se inserido (novo)."""
    cnpj = _normalizar_cnpj(cnpj)
    if not cnpj:
        return False
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.execute(
            "INSERT OR REPLACE INTO clientes_cnpj (cnpj, nome, ativo) VALUES (?, ?, 1)",
            (cnpj, nome),
        )
        conn.commit()
    return cur.rowcount > 0


def listar_cnpjs_clientes() -> set[str]:
    """Retorna conjunto de CNPJs ativos (só dígitos)."""
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            "SELECT cnpj FROM clientes_cnpj WHERE ativo = 1"
        ).fetchall()
    return {row[0] for row in rows}


def total_clientes_cnpj() -> int:
    with sqlite3.connect(DB_PATH) as conn:
        return conn.execute(
            "SELECT COUNT(*) FROM clientes_cnpj WHERE ativo = 1"
        ).fetchone()[0]
