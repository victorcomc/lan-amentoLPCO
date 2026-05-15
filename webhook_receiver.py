"""
Servidor Flask que recebe notificações push do Portal Único Siscomex.

O Portal faz POST neste endpoint quando um evento LPCO ocorre.
O receiver valida o header 'Secret', identifica o tipo de evento e
dispara o email correspondente.

Infraestrutura esperada no servidor Hetzner:
  nginx (porta 443, TLS/Let's Encrypt) → proxy_pass → este app (porta 8080, HTTP)

IPs de origem do Siscomex que devem ser permitidos no firewall:
  161.148.0.0/16 | 189.9.0.0/16 | 200.198.192.0/18
"""

import hmac
import json
import logging
import threading
from flask import Flask, request, jsonify

from config import config
from email_service import notificar_lpco_liberado, notificar_falha_webhook

logger = logging.getLogger(__name__)

app = Flask(__name__)

# ---------------------------------------------------------------------------
# Mapa de eventos LPCO → handler
# Identificadores conforme documentação "Intervenientes Privados" do TALPCO
# ---------------------------------------------------------------------------

# Situações que disparam email para o responsável
SITUACOES_NOTIFICAR = frozenset({
    "DEFERIDO",        # LPCO aprovado — evento principal
    "INDEFERIDO",      # Negado — também queremos saber
})

# Modelos conhecidos
MODELO_FRUTA = "E00144"
MODELO_PESCA = "E00061"

# Evento principal: anuente altera a situação do LPCO
EVENTO_ALTSIT   = "talp-altsit-lpco-anu"

# Outros eventos disponíveis (monitorados no log, sem email por padrão)
EVENTO_EXIG     = "talp-inclusao-exig"
EVENTO_CANC_EX  = "talp-cancela-exig"
EVENTO_PRORROG  = "talp-analise-prorrog"
EVENTO_RETIF    = "talp-analise-retif"
EVENTO_COMPAT   = "talp-analise-compat"
EVENTO_MSG      = "talp-msg-lpco-anu"
EVENTO_RETIFAUT = "talp-retif-automat"
EVENTO_PGTO     = "talp-falha-pagamento"


# ---------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------

@app.route("/webhook/lpco", methods=["POST"])
def receber_notificacao():
    """
    Recebe POST do Portal Único e processa em background.
    Deve responder 200 em < 3500 ms (requisito do portal).
    """
    # Valida chave secreta
    secret = request.headers.get("Secret", "")
    if not _secret_valido(secret):
        logger.warning(
            "Webhook recebido com Secret inválido (IP: %s)", request.remote_addr
        )
        return jsonify({"error": "unauthorized"}), 401

    event_type     = request.headers.get("event-type", "")
    destinatario   = request.headers.get("destinatario-id", "")
    raw_body       = request.get_data()

    logger.info("Evento recebido: type=%s destinatario=%s", event_type, destinatario)

    # ACK imediato — processo em thread separada
    threading.Thread(
        target=_processar,
        args=(event_type, raw_body, destinatario),
        daemon=True,
    ).start()

    return "", 200


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200


# ---------------------------------------------------------------------------
# Processamento assíncrono
# ---------------------------------------------------------------------------

def _secret_valido(received: str) -> bool:
    expected = config.WEBHOOK_SECRET
    if not expected:
        return True  # sem secret configurado: aceita tudo (só dev)
    return hmac.compare_digest(
        received.encode("utf-8"),
        expected.encode("utf-8"),
    )


def _resolver_destinatarios(modelo: str, destinatario_id: str) -> list[str]:
    """Mapeia modelo de LPCO + cert owner para a lista de destinatários de email."""
    if modelo == MODELO_FRUTA:
        return [config.EMAIL_FRUTA]
    if modelo == MODELO_PESCA:
        # Se o cert nordeste (Felipe) estiver configurado e este evento é dele → NE
        if config.CERT_NE_OWNER_ID and destinatario_id == config.CERT_NE_OWNER_ID:
            return [config.EMAIL_PESCA_NE] if config.EMAIL_PESCA_NE else [config.EMAIL_OPERACAO]
        return [config.EMAIL_PESCA_SE]
    logger.warning("Modelo desconhecido '%s' (destinatario=%s) — usando EMAIL_OPERACAO.", modelo, destinatario_id)
    return [config.EMAIL_OPERACAO]


def _processar(event_type: str, raw_body: bytes, destinatario_id: str = "") -> None:
    try:
        payload = json.loads(raw_body) if raw_body else {}
    except json.JSONDecodeError:
        logger.error("Payload inválido (não é JSON): %s", raw_body[:300])
        return

    numero_lpco  = payload.get("numeroLPCO", "")
    codigo_modelo = payload.get("codigoModelo", "")

    if event_type == EVENTO_ALTSIT:
        _handle_alteracao_situacao(numero_lpco, codigo_modelo, payload, destinatario_id)

    elif event_type == EVENTO_EXIG:
        _handle_exigencia(numero_lpco, payload)

    elif event_type == EVENTO_PGTO:
        _handle_falha_pagamento(numero_lpco, payload)

    else:
        logger.info(
            "Evento '%s' para LPCO %s recebido — sem ação configurada.",
            event_type, numero_lpco,
        )


def _handle_alteracao_situacao(numero: str, modelo: str, payload: dict, destinatario_id: str = "") -> None:
    """
    talp-altsit-lpco-anu
    Dispara email quando novaSituacao.id == "DEFERIDO" ou "INDEFERIDO".
    """
    nova_situacao = payload.get("novaSituacao", {})
    situacao_id   = nova_situacao.get("id", "").upper()
    situacao_desc = nova_situacao.get("descricao", situacao_id)
    justificativa = payload.get("justificativa", "")

    logger.info("LPCO %s [%s] — nova situação: %s", numero, modelo, situacao_id)

    if situacao_id not in SITUACOES_NOTIFICAR:
        logger.info("Situação '%s' não requer notificação por email.", situacao_id)
        return

    cnpj_list = payload.get("cpfCnpj", [])
    destinatarios = _resolver_destinatarios(modelo, destinatario_id)
    logger.info("LPCO %s [modelo=%s] → destinatários: %s", numero, modelo, destinatarios)

    detalhes_email = {
        "Número LPCO":    numero,
        "Modelo":         modelo,
        "Nova Situação":  f"{situacao_id} — {situacao_desc}",
        "Justificativa":  justificativa or "(não informada)",
        "CNPJ/CPF":       ", ".join(cnpj_list),
        "Data do evento": payload.get("dataEvento", ""),
    }

    try:
        notificar_lpco_liberado(
            numero_lpco=numero,
            situacao=f"{situacao_id} — {situacao_desc}",
            detalhes=detalhes_email,
            destinatarios=destinatarios,
        )
    except Exception as exc:
        logger.error("Falha ao enviar email para LPCO %s: %s", numero, exc)


def _handle_exigencia(numero: str, payload: dict) -> None:
    """
    talp-inclusao-exig
    Por ora apenas loga — adicione notificação se necessário.
    """
    exigencia = payload.get("exigencia", "")
    logger.info("Exigência incluída no LPCO %s: %s", numero, exigencia[:100])


def _handle_falha_pagamento(numero: str, payload: dict) -> None:
    """
    talp-falha-pagamento
    Notifica a operação sobre falha de pagamento.
    """
    mensagem = payload.get("mensagem", "")
    logger.warning("Falha de pagamento no LPCO %s: %s", numero, mensagem)
    try:
        notificar_falha_webhook(
            f"Falha no pagamento de taxa do LPCO {numero}: {mensagem}"
        )
    except Exception as exc:
        logger.error("Não foi possível enviar alerta de pagamento: %s", exc)


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

def iniciar_servidor() -> None:
    """Inicia o servidor Flask na porta WEBHOOK_PORT."""
    logger.info(
        "Webhook receiver iniciando na porta %d...", config.WEBHOOK_PORT
    )
    app.run(
        host="0.0.0.0",
        port=config.WEBHOOK_PORT,
        debug=False,
        use_reloader=False,
    )
