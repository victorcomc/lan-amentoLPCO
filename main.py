"""
Ponto de entrada do sistema de monitoramento de LPCOs.

Fluxo:
  1. Valida configuração (.env)
  2. Registra/verifica a subscrição webhook no Portal Único
  3. Inicia o servidor Flask que recebe os eventos push
  4. Agendamento periódico verifica saúde da subscrição (a cada N horas)
"""

import logging
import sys
import os
from apscheduler.schedulers.background import BackgroundScheduler

from config import config
from webhook_manager import WebhookManager
from webhook_receiver import iniciar_servidor
from email_service import notificar_falha_webhook

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("lpco_monitor.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)


def _url_webhook() -> str:
    """
    Monta a URL pública do endpoint de webhook.
    Defina WEBHOOK_PUBLIC_URL no .env ou sobrescreva aqui.
    Ex: https://meuservidor.com.br/webhook/lpco
    """
    url = os.getenv("WEBHOOK_PUBLIC_URL", "")
    if not url:
        raise RuntimeError(
            "WEBHOOK_PUBLIC_URL não configurada no .env. "
            "Defina o endereço HTTPS público do servidor (ex: https://meudominio.com/webhook/lpco)."
        )
    return url


def verificar_saude_webhook() -> None:
    """Job periódico: confirma que a subscrição está ativa."""
    mgr = WebhookManager(url_publica=_url_webhook())
    ok = mgr.garantir_subscricao_ativa()
    if not ok:
        logger.error("Subscrição webhook inativa e não foi possível recriar.")
        try:
            notificar_falha_webhook(
                "Subscrição webhook LPCO está inativa no Portal Único. "
                "Autenticação ou registro falhou — verifique WEBHOOK_ROLE_TYPE."
            )
        except Exception as exc:
            logger.error("Falha ao enviar alerta: %s", exc)


def main() -> None:
    logger.info("=== Monitor LPCO iniciando ===")

    # 1. Valida .env
    try:
        config.validate()
    except (ValueError, FileNotFoundError) as exc:
        logger.critical("Configuração inválida: %s", exc)
        sys.exit(1)

    # 2. Registra/verifica subscrição no portal
    try:
        url = _url_webhook()
    except RuntimeError as exc:
        logger.critical(str(exc))
        sys.exit(1)

    mgr = WebhookManager(url_publica=url)
    if not mgr.garantir_subscricao_ativa():
        logger.critical(
            "Não foi possível registrar o webhook. "
            "Verifique WEBHOOK_ROLE_TYPE no .env e tente novamente."
        )
        sys.exit(1)

    # 3. Agenda verificação de saúde
    scheduler = BackgroundScheduler(timezone="America/Sao_Paulo")
    scheduler.add_job(
        verificar_saude_webhook,
        "interval",
        hours=config.WEBHOOK_HEALTH_CHECK_HOURS,
        id="health_check",
    )
    scheduler.start()
    logger.info(
        "Verificação de saúde agendada a cada %dh.",
        config.WEBHOOK_HEALTH_CHECK_HOURS,
    )

    # 4. Sobe o receiver Flask (bloqueante — mantém o processo vivo)
    logger.info("Iniciando receiver na porta %d...", config.WEBHOOK_PORT)
    try:
        iniciar_servidor()
    finally:
        scheduler.shutdown()
        logger.info("=== Monitor LPCO encerrado ===")


if __name__ == "__main__":
    main()
