"""
Sincronização da planilha SharePoint → banco local de LPCOs.

Lê a coluna B da aba "Ibama" do arquivo Excel no SharePoint via
Microsoft Graph API (OAuth2 client credentials — sem login do usuário).
"""

import logging
from typing import Any
from urllib.parse import quote

import requests

from config import config
from database import registrar_lpco, total_lpcos

logger = logging.getLogger(__name__)

_GRAPH_BASE = "https://graph.microsoft.com/v1.0"
_TOKEN_URL  = "https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token"


def _obter_token() -> str:
    resp = requests.post(
        _TOKEN_URL.format(tenant=config.GRAPH_TENANT_ID),
        data={
            "grant_type":    "client_credentials",
            "client_id":     config.GRAPH_CLIENT_ID,
            "client_secret": config.GRAPH_CLIENT_SECRET,
            "scope":         "https://graph.microsoft.com/.default",
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def _get(token: str, url: str, **kwargs: Any) -> Any:
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    resp = requests.get(url, headers=headers, timeout=60, **kwargs)
    resp.raise_for_status()
    return resp.json()


def _obter_site_id(token: str) -> str:
    data = _get(token, f"{_GRAPH_BASE}/sites/{config.GRAPH_SITE_PATH}")
    return data["id"]


def _obter_aba_ibama(token: str, site_id: str) -> str:
    """Retorna o ID da aba que contém 'ibama' no nome (case-insensitive)."""
    data = _get(
        token,
        f"{_GRAPH_BASE}/sites/{site_id}/drive/items/{config.GRAPH_FILE_ID}/workbook/worksheets",
    )
    for sheet in data.get("value", []):
        if "ibama" in sheet.get("name", "").lower():
            sheet_id = sheet["id"]
            logger.info("Aba encontrada: '%s' (id=%s)", sheet["name"], sheet_id)
            return sheet_id
    raise ValueError(
        f"Aba com 'Ibama' não encontrada. Abas disponíveis: "
        + ", ".join(s.get("name", "") for s in data.get("value", []))
    )


def _ler_coluna_b(token: str, site_id: str, sheet_id: str) -> list[str]:
    """
    Lê usedRange da aba e extrai valores da coluna B (índice 1).
    Ignora cabeçalho (primeira linha) e células vazias.
    """
    encoded_id = quote(sheet_id, safe="")
    data = _get(
        token,
        f"{_GRAPH_BASE}/sites/{site_id}/drive/items/{config.GRAPH_FILE_ID}"
        f"/workbook/worksheets/{encoded_id}/usedRange",
        params={"$select": "values"},
    )
    rows = data.get("values") or []
    numeros: list[str] = []
    for row in rows[1:]:  # pula cabeçalho
        if len(row) > 1:
            val = str(row[1]).strip().upper()
            if val and val not in ("", "NONE", "NULL") and not val.startswith("#"):
                numeros.append(val)
    return numeros


def sincronizar_planilha() -> None:
    """
    Lê a planilha SharePoint e registra os números de LPCO no banco local.
    Chamado na inicialização e a cada hora pelo scheduler.
    """
    if not all([
        config.GRAPH_TENANT_ID,
        config.GRAPH_CLIENT_ID,
        config.GRAPH_CLIENT_SECRET,
        config.GRAPH_FILE_ID,
        config.GRAPH_SITE_PATH,
    ]):
        logger.warning("Variáveis GRAPH_* não configuradas — sincronização ignorada.")
        return

    logger.info("Iniciando sincronização com SharePoint...")
    try:
        token    = _obter_token()
        site_id  = _obter_site_id(token)
        sheet_id = _obter_aba_ibama(token, site_id)
        numeros  = _ler_coluna_b(token, site_id, sheet_id)
    except Exception as exc:
        logger.error("Falha na sincronização com SharePoint: %s", exc)
        return

    novos = sum(1 for n in numeros if registrar_lpco(n))
    logger.info(
        "Sync SharePoint concluída: %d número(s) na planilha, %d novo(s) registrado(s). "
        "Total no banco: %d.",
        len(numeros), novos, total_lpcos(),
    )
