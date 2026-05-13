"""
Gerencia a subscrição de webhooks no Portal Único Siscomex.

Endpoints utilizados (todos precisam de auth — cookie JWTPCMX_USR):
  POST   /portal/api/ext/webhook          — subscrever
  GET    /portal/api/ext/webhook          — listar subscrições
  PUT    /portal/api/ext/webhook/{id}     — atualizar
  DELETE /portal/api/ext/webhook/{id}     — excluir
  GET    /portal/api/ext/webhook/falhas   — consultar falhas

A autenticação exige chamar primeiro /portal/api/autenticar com o
header Role-Type correto (ver WEBHOOK_ROLE_TYPE no .env).
"""

import logging
from dataclasses import dataclass
from typing import Any

import requests

from config import config
from siscomex_client import SiscomexClient

logger = logging.getLogger(__name__)

# Evento principal: anuente altera a situação do LPCO (intervenientes privados)
EVENTO_LPCO = "talp-altsit-lpco-anu"

_WEBHOOK_PATH = "/portal/api/ext/webhook"


@dataclass
class Subscricao:
    id: int
    evento: str
    endpoint: str
    ativa: bool = True
    motivo_exclusao: str | None = None


class WebhookManager:
    """
    Gerencia o ciclo de vida da subscrição de eventos LPCO no portal.

    Uso típico (chamado na inicialização do sistema):
        mgr = WebhookManager(url_publica="https://meuservidor.com/webhook/lpco")
        mgr.garantir_subscricao_ativa()
    """

    def __init__(self, url_publica: str) -> None:
        self._url_publica = url_publica

    # ------------------------------------------------------------------
    # API pública
    # ------------------------------------------------------------------

    def garantir_subscricao_ativa(self) -> bool:
        """
        Verifica se há subscrição ativa. Se não houver, cria uma.
        Retorna True se tudo estiver OK.
        """
        with SiscomexClient() as client:
            if not client.autenticar(config.WEBHOOK_ROLE_TYPE):
                logger.error(
                    "Não foi possível autenticar para gerenciar webhook. "
                    "Verifique WEBHOOK_ROLE_TYPE no .env."
                )
                return False

            subs = self._listar(client)
            ativas = [s for s in subs if s.ativa and s.evento == EVENTO_LPCO]

            if ativas:
                logger.info(
                    "Subscrição LPCO já ativa (id=%d, endpoint=%s).",
                    ativas[0].id,
                    ativas[0].endpoint,
                )
                return True

            logger.info("Nenhuma subscrição ativa para %s. Criando...", EVENTO_LPCO)
            nova = self._criar(client)
            if nova:
                logger.info(
                    "Subscrição criada com sucesso (id=%d).", nova.id
                )
                return True

            logger.error("Falha ao criar subscrição de webhook.")
            return False

    def verificar_falhas(self) -> list[dict]:
        """Retorna falhas recentes de entrega (últimas 24h)."""
        with SiscomexClient() as client:
            if not client.autenticar(config.WEBHOOK_ROLE_TYPE):
                return []
            return self._falhas(client)

    # ------------------------------------------------------------------
    # Chamadas HTTP internas
    # ------------------------------------------------------------------

    def _listar(self, client: SiscomexClient) -> list[Subscricao]:
        try:
            dados = client._get(  # type: ignore[attr-defined]
                _WEBHOOK_PATH, params={"exibirInativos": "false"}
            )
            return [
                Subscricao(
                    id=item["id"],
                    evento=item["evento"],
                    endpoint=item["endpoint"],
                    ativa=not item.get("dataExclusao"),
                )
                for item in (dados if isinstance(dados, list) else [])
            ]
        except requests.HTTPError as exc:
            logger.error("Erro ao listar subscrições: %s", exc)
            return []

    def _criar(self, client: SiscomexClient) -> Subscricao | None:
        payload: dict[str, Any] = {
            "evento": EVENTO_LPCO,
            "endpoint": self._url_publica,
            "chaveSecreta": config.WEBHOOK_SECRET,
        }
        try:
            resp = client._post(_WEBHOOK_PATH, payload)  # type: ignore[attr-defined]
            return Subscricao(
                id=resp["id"],  # type: ignore[index]
                evento=resp["evento"],  # type: ignore[index]
                endpoint=resp["endpoint"],  # type: ignore[index]
            )
        except requests.HTTPError as exc:
            logger.error("Erro ao criar subscrição: HTTP %s — %s", exc.response.status_code, exc.response.text[:200])
            return None

    def _falhas(self, client: SiscomexClient) -> list[dict]:
        try:
            dados = client._get(f"{_WEBHOOK_PATH}/falhas")  # type: ignore[attr-defined]
            return dados if isinstance(dados, list) else []  # type: ignore[return-value]
        except requests.HTTPError as exc:
            logger.error("Erro ao consultar falhas: %s", exc)
            return []
