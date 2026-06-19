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

# Evento DUE: qualquer alteração de estado da Declaração Única de Exportação
EVENTO_DUE = "duex-historico"

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
        Garante subscrições ativas para SE e, se configurado, NE.
        Retorna True somente se todas as subscrições necessárias estiverem OK.
        """
        ok_se = self._garantir_cert(
            config.CERT_PFX_PATH,
            config.CERT_PFX_BASE64,
            config.CERT_PFX_PASSWORD,
            "SE",
        )

        if config.CERT_NE_PFX_BASE64 or config.CERT_NE_PFX_PATH:
            ok_ne = self._garantir_cert(
                config.CERT_NE_PFX_PATH,
                config.CERT_NE_PFX_BASE64,
                config.CERT_NE_PFX_PASSWORD,
                "NE",
            )
        else:
            logger.info("Certificado NE não configurado — subscrição NE ignorada.")
            ok_ne = True

        return ok_se and ok_ne

    def _garantir_cert(
        self, pfx_path: str, pfx_base64: str, pfx_password: str, regiao: str
    ) -> bool:
        with SiscomexClient(
            cert_pfx_path=pfx_path,
            cert_pfx_base64=pfx_base64,
            cert_pfx_password=pfx_password,
        ) as client:
            if not client.autenticar(config.WEBHOOK_ROLE_TYPE):
                logger.error("Autenticação falhou para certificado %s.", regiao)
                return False

            subs = self._listar(client)
            ativas = [s for s in subs if s.ativa and s.evento == EVENTO_LPCO]

            corretas = [s for s in ativas if s.endpoint == self._url_publica]
            if corretas:
                logger.info(
                    "Subscrição LPCO %s já ativa (id=%d, endpoint=%s).",
                    regiao, corretas[0].id, corretas[0].endpoint,
                )
                return True

            # Endpoint errado — remove e recria
            for s in ativas:
                logger.warning(
                    "Subscrição %s com endpoint incorreto '%s' (id=%d). Removendo...",
                    regiao, s.endpoint, s.id,
                )
                self._deletar(client, s.id)

            logger.info("Criando subscrição %s para %s...", EVENTO_LPCO, regiao)
            nova = self._criar(client)
            if nova:
                logger.info("Subscrição %s criada (id=%d).", regiao, nova.id)
                return True

            logger.error("Falha ao criar subscrição %s.", regiao)
            return False

    def garantir_subscricao_due(self) -> bool:
        """
        Garante subscrição ativa para eventos DUE (duex-historico) com o certificado SE.
        Independente das subscrições LPCO — não as afeta.
        """
        return self._garantir_cert_evento(
            config.CERT_PFX_PATH,
            config.CERT_PFX_BASE64,
            config.CERT_PFX_PASSWORD,
            EVENTO_DUE,
            "DUE-SE",
        )

    def _garantir_cert_evento(
        self,
        pfx_path: str,
        pfx_base64: str,
        pfx_password: str,
        evento: str,
        label: str,
    ) -> bool:
        """Garante subscrição ativa para um evento específico. Reutilizável."""
        with SiscomexClient(
            cert_pfx_path=pfx_path,
            cert_pfx_base64=pfx_base64,
            cert_pfx_password=pfx_password,
        ) as client:
            if not client.autenticar(config.WEBHOOK_ROLE_TYPE):
                logger.error("Autenticação falhou para subscrição %s.", label)
                return False

            subs   = self._listar(client)
            ativas  = [s for s in subs if s.ativa and s.evento == evento]
            corretas = [s for s in ativas if s.endpoint == self._url_publica]

            if corretas:
                logger.info(
                    "Subscrição %s já ativa (id=%d, endpoint=%s).",
                    label, corretas[0].id, corretas[0].endpoint,
                )
                return True

            for s in ativas:
                logger.warning(
                    "Subscrição %s com endpoint incorreto '%s' (id=%d). Removendo...",
                    label, s.endpoint, s.id,
                )
                self._deletar(client, s.id)

            logger.info("Criando subscrição %s para evento %s...", label, evento)
            nova = self._criar_evento(client, evento)
            if nova:
                logger.info("Subscrição %s criada (id=%d).", label, nova.id)
                return True

            logger.error("Falha ao criar subscrição %s.", label)
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
        return self._criar_evento(client, EVENTO_LPCO)

    def _criar_evento(self, client: SiscomexClient, evento: str) -> Subscricao | None:
        payload: dict[str, Any] = {
            "evento": evento,
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
            logger.error("Erro ao criar subscrição '%s': HTTP %s — %s", evento, exc.response.status_code, exc.response.text[:200])
            return None

    def _deletar(self, client: SiscomexClient, subscricao_id: int) -> None:
        try:
            client._delete(f"{_WEBHOOK_PATH}/{subscricao_id}")  # type: ignore[attr-defined]
            logger.info("Subscrição %d removida.", subscricao_id)
        except requests.HTTPError as exc:
            logger.error("Erro ao remover subscrição %d: %s", subscricao_id, exc)

    def _falhas(self, client: SiscomexClient) -> list[dict]:
        try:
            dados = client._get(f"{_WEBHOOK_PATH}/falhas")  # type: ignore[attr-defined]
            return dados if isinstance(dados, list) else []  # type: ignore[return-value]
        except requests.HTTPError as exc:
            logger.error("Erro ao consultar falhas: %s", exc)
            return []
