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

import csv
import hmac
import io
import json
import logging
import threading
from flask import Flask, Response, request, jsonify

from config import config
from database import lpco_conhecido, registrar_lpco, registrar_evento, listar_eventos
from email_service import notificar_lpco_liberado, notificar_falha_webhook

logger = logging.getLogger(__name__)

app = Flask(__name__)

# ---------------------------------------------------------------------------
# Mapa de eventos LPCO → handler
# Identificadores conforme documentação "Intervenientes Privados" do TALPCO
# ---------------------------------------------------------------------------


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

_COLUNAS_CSV = [
    "data_evento", "data_recebido", "numero_lpco", "codigo_modelo",
    "tipo", "regiao", "destinatario_id", "situacao_id", "situacao_desc",
    "justificativa", "cnpj_cpf",
]


# ---------------------------------------------------------------------------
# Endpoints
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


@app.route("/lpco/registrar", methods=["POST"])
def registrar_lpco_endpoint():
    """
    Chamado pelo Power Automate quando um novo LPCO é adicionado na planilha.
    Body JSON: {"numero": "E2600287640"}
    Header: Secret: <WEBHOOK_SECRET>
    """
    secret = request.headers.get("Secret", "")
    if not _secret_valido(secret):
        return jsonify({"error": "unauthorized"}), 401

    data = request.get_json(silent=True) or {}
    numero = str(data.get("numero", "")).strip().upper()
    if not numero:
        return jsonify({"error": "campo 'numero' obrigatorio"}), 400

    novo = registrar_lpco(numero)
    total = __import__("database").total_lpcos()
    logger.info("LPCO %s %s via Power Automate. Total no banco: %d.", numero, "registrado" if novo else "já existia", total)
    return jsonify({"ok": True, "numero": numero, "novo": novo}), 200


@app.route("/relatorio/csv", methods=["GET"])
def relatorio_csv():
    """
    Gera CSV com todos os eventos de alteração de situação registrados no banco.

    Parâmetros opcionais:
      ?de=YYYY-MM-DD   — data inicial (padrão: sem filtro)
      ?ate=YYYY-MM-DD  — data final   (padrão: sem filtro)

    Autenticação: header Secret (mesmo secret do webhook).
    """
    secret = request.headers.get("Secret", "")
    if not _secret_valido(secret):
        return jsonify({"error": "unauthorized"}), 401

    de  = request.args.get("de", "")
    ate = request.args.get("ate", "")

    eventos = listar_eventos(de, ate)

    output = io.StringIO()
    writer = csv.DictWriter(
        output, fieldnames=_COLUNAS_CSV, extrasaction="ignore", lineterminator="\r\n"
    )
    writer.writeheader()
    writer.writerows(eventos)

    sufixo = f"{de or 'inicio'}_{ate or 'hoje'}"
    return Response(
        "﻿" + output.getvalue(),           # BOM UTF-8 para Excel abrir corretamente
        mimetype="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="relatorio_lpco_{sufixo}.csv"'},
    )


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
    """Retorna destinatários de email. Fruta será configurada futuramente."""
    return [config.EMAIL_PESCA]


def _processar(event_type: str, raw_body: bytes, destinatario_id: str = "") -> None:
    try:
        payload = json.loads(raw_body) if raw_body else {}
    except json.JSONDecodeError:
        logger.error("Payload inválido (não é JSON): %s", raw_body[:300])
        return

    numero_lpco  = payload.get("numeroLPCO", "")
    codigo_modelo = payload.get("codigoModelo", "")

    # Log completo para análise dos campos reais do payload
    logger.info(
        "=== PAYLOAD COMPLETO === event=%s destinatario-id=%s numeroLPCO=%s codigoModelo=%s payload=%s",
        event_type, destinatario_id, numero_lpco, codigo_modelo,
        json.dumps(payload, ensure_ascii=False),
    )

    if event_type == EVENTO_ALTSIT:
        if not lpco_conhecido(numero_lpco):
            logger.info("LPCO %s ignorado — não registrado como processo da Hevile.", numero_lpco)
            return
        _handle_alteracao_situacao(numero_lpco, codigo_modelo, payload, destinatario_id)

    elif event_type == "duex-historico":
        _handle_due(payload)

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
    """talp-altsit-lpco-anu — dispara email e persiste o evento no banco."""
    nova_situacao = payload.get("novaSituacao", {})
    situacao_id   = nova_situacao.get("id", "").upper()
    situacao_desc = nova_situacao.get("descricao", situacao_id)
    justificativa = payload.get("justificativa", "")

    logger.info("LPCO %s [%s] — nova situação: %s", numero, modelo, situacao_id)

    cnpj_list     = payload.get("cpfCnpj", [])
    destinatarios = _resolver_destinatarios(modelo, destinatario_id)
    regiao        = "NE" if (config.CERT_NE_OWNER_ID and destinatario_id == config.CERT_NE_OWNER_ID) else "SE"
    tipo_label    = f"Pesca {regiao}"

    logger.info("LPCO %s [%s] → destinatários: %s", numero, tipo_label, destinatarios)

    # Persiste no banco para histórico / relatórios
    try:
        registrar_evento({
            "data_evento":     payload.get("dataEvento", ""),
            "numero_lpco":     numero,
            "codigo_modelo":   modelo,
            "tipo":            tipo_label,
            "regiao":          regiao,
            "destinatario_id": destinatario_id,
            "situacao_id":     situacao_id,
            "situacao_desc":   situacao_desc,
            "justificativa":   justificativa,
            "cnpj_cpf":        ", ".join(cnpj_list),
            "payload_json":    json.dumps(payload, ensure_ascii=False),
        })
    except Exception as exc:
        logger.error("Falha ao persistir evento %s no banco: %s", numero, exc)

    detalhes_email = {
        "Número LPCO":    numero,
        "Tipo":           tipo_label,
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
            tipo=tipo_label,
        )
    except Exception as exc:
        logger.error("Falha ao enviar email para LPCO %s: %s", numero, exc)


def _handle_due(payload: dict) -> None:
    """
    duex-historico — recebido quando uma DUE muda de estado.

    Payload: {"tipo": "DESEMBARACADA", "descricao": "...", "data": "...",
              "due": {"numero": "26BR...", "ruc": "6BR..."}}

    Após receber o número, tenta consultar o detalhe via API para descobrir
    o endpoint correto e logar todos os campos disponíveis (FOB, frete, peso, etc.).
    """
    due_obj   = payload.get("due") or {}
    numero    = due_obj.get("numero", "")
    ruc       = due_obj.get("ruc", "")
    tipo      = payload.get("tipo", "")
    descricao = payload.get("descricao", "")
    data_ev   = payload.get("data", "")

    logger.info(
        "=== DUE RECEBIDA === numero=%s ruc=%s tipo=%s descricao=%s data=%s",
        numero, ruc, tipo, descricao, data_ev,
    )

    if not numero:
        logger.warning("Evento duex-historico sem número de DUE — ignorando.")
        return

    # Consulta detalhe em thread separada para não atrasar o ACK
    threading.Thread(
        target=_consultar_e_logar_due,
        args=(numero,),
        daemon=True,
    ).start()


def _consultar_e_logar_due(numero: str) -> None:
    """Abre sessão e tenta obter o detalhe completo da DUE."""
    from siscomex_client import SiscomexClient
    try:
        with SiscomexClient() as client:
            if not client.autenticar(config.WEBHOOK_ROLE_TYPE):
                logger.error("DUE %s: autenticação falhou para consulta de detalhe.", numero)
                return
            detalhe = client.consultar_due(numero)
            if detalhe:
                logger.info(
                    "=== DUE DETALHE === numero=%s dados=%s",
                    numero, json.dumps(detalhe, ensure_ascii=False),
                )
            else:
                logger.warning(
                    "DUE %s: detalhe não disponível ainda. "
                    "Endpoint será descoberto conforme eventos chegarem.",
                    numero,
                )
    except Exception as exc:
        logger.error("DUE %s: erro ao consultar detalhe: %s", numero, exc)


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
