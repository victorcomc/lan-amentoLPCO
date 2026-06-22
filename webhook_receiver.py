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
from database import (
    lpco_conhecido, registrar_lpco, registrar_evento, listar_eventos,
    registrar_due, atualizar_due_detalhe,
    registrar_cnpj_cliente, listar_cnpjs_clientes, total_clientes_cnpj,
)
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


@app.route("/cliente/registrar", methods=["POST"])
def registrar_cliente_endpoint():
    """
    Registra um ou mais CNPJs de clientes para filtro do relatório semanal.

    Body JSON — um CNPJ:
        {"cnpj": "12345678000199", "nome": "Empresa X"}

    Body JSON — lista:
        {"clientes": [{"cnpj": "...", "nome": "..."}, ...]}

    Header: Secret: <WEBHOOK_SECRET>
    """
    secret = request.headers.get("Secret", "")
    if not _secret_valido(secret):
        return jsonify({"error": "unauthorized"}), 401

    data = request.get_json(silent=True) or {}
    inseridos = 0

    # aceita lista ou item único
    clientes = data.get("clientes") or (
        [{"cnpj": data["cnpj"], "nome": data.get("nome", "")}] if data.get("cnpj") else []
    )

    for item in clientes:
        cnpj = str(item.get("cnpj", "")).strip()
        nome = str(item.get("nome", "")).strip()
        if registrar_cnpj_cliente(cnpj, nome):
            inseridos += 1

    total = total_clientes_cnpj()
    logger.info("CNPJs clientes: %d inseridos. Total ativo: %d.", inseridos, total)
    return jsonify({"ok": True, "inseridos": inseridos, "total_clientes": total}), 200


@app.route("/cliente/listar", methods=["GET"])
def listar_clientes_endpoint():
    """Lista todos os CNPJs de clientes cadastrados."""
    secret = request.headers.get("Secret", "")
    if not _secret_valido(secret):
        return jsonify({"error": "unauthorized"}), 401
    cnpjs = sorted(listar_cnpjs_clientes())
    return jsonify({"total": len(cnpjs), "cnpjs": cnpjs}), 200


_relatorio_lock = threading.Lock()

# ---------------------------------------------------------------------------
# Cache de tabelas TABX — carregado uma vez na primeira DUE recebida
# ---------------------------------------------------------------------------
_CACHE_PAISES: dict[str, str]   = {}   # str(codigo_int) → nome ex: "190" → "COREIA DO SUL"
_CACHE_RECINTOS: dict[str, str] = {}   # codigo_str → nome ex: "8911101" → "AEROPORTO..."
_tabelas_lock = threading.Lock()
_tabelas_carregadas = False


def _garantir_tabelas(client) -> None:
    """Carrega tabelas TABX de países e recintos se ainda não carregadas."""
    global _tabelas_carregadas
    with _tabelas_lock:
        if _tabelas_carregadas:
            return

        # Países — possíveis nomes de tabela no TABX
        for nome_pais in ("PAIS", "PAISES", "PAIS_BACEN"):
            try:
                registros = client.consultar_tabela_comex(nome_pais)
                if not registros:
                    continue
                # detecta os campos automaticamente (CODIGO/codigo, NOME/nome/DESCRICAO)
                for r in registros:
                    codigo = (r.get("CODIGO") or r.get("codigo") or
                              r.get("CODIGO_PAIS") or r.get("codigo_pais", ""))
                    nome   = (r.get("NOME") or r.get("nome") or
                              r.get("DESCRICAO") or r.get("descricao", ""))
                    if codigo and nome:
                        _CACHE_PAISES[str(codigo).strip()] = nome.strip()
                if _CACHE_PAISES:
                    logger.info("TABX: %d países carregados da tabela '%s'.", len(_CACHE_PAISES), nome_pais)
                    break
            except Exception as exc:
                logger.debug("TABX %s: %s", nome_pais, exc)

        if not _CACHE_PAISES:
            logger.warning("TABX: tabela de países não encontrada — pais_destino mostrará o código numérico.")

        # Recintos — possíveis nomes de tabela
        for nome_rec in ("RECINTO_ADUANEIRO", "RECINTO", "RECINTO_ALFANDEGADO"):
            try:
                registros = client.consultar_tabela_comex(nome_rec)
                if not registros:
                    continue
                for r in registros:
                    codigo = (r.get("CODIGO") or r.get("codigo") or
                              r.get("CODIGO_RECINTO") or r.get("codigo_recinto", ""))
                    nome   = (r.get("NOME") or r.get("nome") or
                              r.get("DESCRICAO") or r.get("descricao", ""))
                    if codigo and nome:
                        _CACHE_RECINTOS[str(codigo).strip()] = nome.strip()
                if _CACHE_RECINTOS:
                    logger.info("TABX: %d recintos carregados da tabela '%s'.", len(_CACHE_RECINTOS), nome_rec)
                    break
            except Exception as exc:
                logger.debug("TABX %s: %s", nome_rec, exc)

        if not _CACHE_RECINTOS:
            logger.warning("TABX: tabela de recintos não encontrada — porto_embarque mostrará o código.")

        _tabelas_carregadas = True


@app.route("/relatorio/enviar", methods=["GET", "POST"])
def enviar_relatorio_agora():
    """
    Dispara o relatório semanal imediatamente.
    Acesse pelo browser: https://seuservidor.com/relatorio/enviar?secret=VALOR
    """
    secret = request.args.get("secret") or request.headers.get("Secret", "")
    if not _secret_valido(secret):
        return "<h2>❌ Secret inválido</h2>", 401

    if not _relatorio_lock.acquire(blocking=False):
        logger.warning("Relatório já está em execução — requisição ignorada.")
        return "<h2>⏳ Relatório já está em geração — aguarde o email.</h2>", 200

    def _run():
        try:
            from relatorio_semanal import gerar_e_enviar_relatorio_semanal
            gerar_e_enviar_relatorio_semanal()
        except Exception as exc:
            logger.error("Relatório semanal falhou com exceção não tratada: %s", exc, exc_info=True)
            try:
                notificar_falha_webhook(
                    f"Relatório semanal falhou com erro crítico:\n\n{exc}\n\nVerifique os logs do container."
                )
            except Exception as email_exc:
                logger.error("Não foi possível enviar alerta de falha do relatório: %s", email_exc)
        finally:
            _relatorio_lock.release()

    threading.Thread(target=_run, daemon=True).start()
    logger.info("Relatório semanal disparado manualmente via /relatorio/enviar.")
    return "<h2>✅ Relatório em geração — chegará por email em alguns minutos.</h2>", 200


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

    Salva imediatamente no banco para pesquisa de mercado, depois tenta
    enriquecer consultando o detalhe via API em background.
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

    # Persiste o evento imediatamente (campos básicos do webhook)
    due_id = 0
    try:
        due_id = registrar_due({
            "data_evento":     data_ev,
            "numero_due":      numero,
            "ruc":             ruc,
            "tipo_evento":     tipo,
            "descricao_evento": descricao,
            "payload_json":    json.dumps(payload, ensure_ascii=False),
        })
        logger.info("DUE %s salva no banco (id=%d). Buscando detalhe...", numero, due_id)
    except Exception as exc:
        logger.error("DUE %s: erro ao salvar no banco: %s", numero, exc)

    # Enriquece com detalhe da API em thread separada
    threading.Thread(
        target=_consultar_e_enriquecer_due,
        args=(numero, due_id),
        daemon=True,
    ).start()


def _consultar_e_enriquecer_due(numero: str, due_id: int) -> None:
    """Consulta o detalhe da DUE via API e enriquece o registro no banco."""
    from siscomex_client import SiscomexClient
    try:
        with SiscomexClient() as client:
            if not client.autenticar(config.WEBHOOK_ROLE_TYPE):
                logger.error("DUE %s: autenticação falhou para consulta de detalhe.", numero)
                return
            _garantir_tabelas(client)

            detalhe = client.consultar_due(numero)
            if not detalhe:
                logger.warning("DUE %s: detalhe não disponível via API.", numero)
                return

            logger.info(
                "=== DUE DETALHE === numero=%s dados=%s",
                numero, json.dumps(detalhe, ensure_ascii=False),
            )

            # Extrai campos do detalhe para enriquecer o banco
            campos = _extrair_campos_due(detalhe)
            campos["detalhe_json"] = json.dumps(detalhe, ensure_ascii=False)

            if due_id:
                try:
                    atualizar_due_detalhe(due_id, campos)
                    logger.info(
                        "DUE %s enriquecida no banco: exportador=%s pais=%s peso=%.1fkg",
                        numero,
                        campos.get("exportador_nome", "?"),
                        campos.get("pais_destino", "?"),
                        campos.get("peso_liquido_kg") or 0,
                    )
                except Exception as exc:
                    logger.error("DUE %s: erro ao atualizar detalhe no banco: %s", numero, exc)

    except Exception as exc:
        logger.error("DUE %s: erro ao consultar detalhe: %s", numero, exc)


def _extrair_campos_due(detalhe: dict) -> dict:
    """
    Extrai campos da resposta de GET /due/api/ext/due/numero-da-due/{numero}.

    Detecta automaticamente se o payload é DUE completa ou DUEResumida
    (endpoint consultarDadosResumidosDUE — estrutura diferente).

    DUE completa:
      itens[].exportador.numero (ou .numeroDoDocumento) → CNPJ exportador
      itens[].ncm.codigo / .descricao                   → NCM
      itens[].pesoLiquidoTotal (soma)                   → peso total
      itens[].listaPaisDestino[].nome                   → país destino
      recintoAduaneiroDeEmbarque.descricao              → porto
      valorTotalMercadoria                              → FOB total
      situacao                                          → status da DUE
      dataDaAverbacao                                   → data de averbação
      canal                                             → VERDE/LARANJA/VERMELHO

    DUEResumida:
      exportadores[].numero → CNPJ exportador
      situacaoDUE (int)     → código de situação
      dataSituacaoDUE       → data da situação
    """
    # Detecta formato DUEResumida pelo campo discriminador
    if "numeroDUE" in detalhe or ("exportadores" in detalhe and "itens" not in detalhe):
        return _extrair_campos_due_resumida(detalhe)

    itens = detalhe.get("itens") or []
    primeiro = itens[0] if itens else {}

    # Exportador CNPJ — itens[].exportador.numeroDoDocumento (API real não usa 'numero')
    # Fallback: declarante.numeroDoDocumento no nível raiz
    exportador_cnpj = ""
    exportador_nome = ""
    for item in itens:
        exp = item.get("exportador") or {}
        doc = exp.get("numero") or exp.get("numeroDoDocumento", "")
        if doc:
            exportador_cnpj = doc
            break
    if not exportador_cnpj:
        declarante = detalhe.get("declarante") or {}
        exportador_cnpj = declarante.get("numero") or declarante.get("numeroDoDocumento", "")

    # Exportador nome — a API NÃO inclui 'nome' em itens[].exportador
    # Usa declarante.nome do nível raiz (declarante = quem registrou a DUE = exportador)
    declarante = detalhe.get("declarante") or {}
    exportador_nome = declarante.get("nome", "")

    # NCM e descrição — primeiro item
    ncm_obj    = primeiro.get("ncm") or {}
    ncm_codigo = ncm_obj.get("codigo", "")
    ncm_desc   = ncm_obj.get("descricao", "") or primeiro.get("descricaoDaMercadoria", "")

    # Peso líquido total (soma de todos os itens)
    try:
        peso_liquido = sum(float(item.get("pesoLiquidoTotal") or 0) for item in itens)
    except (TypeError, ValueError):
        peso_liquido = 0.0

    # Valor FOB total
    try:
        fob = float(detalhe.get("valorTotalMercadoria") or 0)
        if not fob and primeiro:
            fob = float(primeiro.get("valorDaMercadoriaNaCondicaoDeVenda") or 0)
    except (TypeError, ValueError):
        fob = 0.0

    # País destino — listaPaisDestino só retorna {"codigo": N}, sem nome
    # 1) Tenta no cache TABX, 2) enderecoImportador, 3) paisImportador.nome
    pais = ""
    paises_lista = primeiro.get("listaPaisDestino") or []
    if paises_lista and isinstance(paises_lista[0], dict):
        codigo_pais = str(paises_lista[0].get("codigo", "")).strip()
        pais = (_CACHE_PAISES.get(codigo_pais)
                or paises_lista[0].get("nome", ""))
    if not pais:
        endereco = primeiro.get("enderecoImportador", "")
        if "EXTERIOR - " in endereco:
            pais = endereco.split("EXTERIOR - ")[-1].strip()
    if not pais:
        pais_imp = detalhe.get("paisImportador") or {}
        pais = pais_imp.get("nome", "") if isinstance(pais_imp, dict) else ""

    # Porto de embarque — recintoAduaneiroDeEmbarque só retorna {"codigo": "..."}, sem descricao
    # 1) Tenta no cache TABX, 2) .descricao, 3) código bruto
    recinto_emb = detalhe.get("recintoAduaneiroDeEmbarque") or {}
    if isinstance(recinto_emb, dict):
        codigo_recinto = str(recinto_emb.get("codigo", "")).strip()
        porto = (recinto_emb.get("descricao")
                 or _CACHE_RECINTOS.get(codigo_recinto)
                 or codigo_recinto)
    else:
        porto = ""

    return {
        "exportador_cnpj": exportador_cnpj,
        "exportador_nome": exportador_nome,
        "produto_ncm":     ncm_codigo,
        "produto_desc":    ncm_desc,
        "peso_liquido_kg": round(peso_liquido, 3) if peso_liquido else None,
        "valor_fob_usd":   round(fob, 2) if fob else None,
        "pais_destino":    pais,
        "porto_embarque":  porto,
        "situacao_due":    detalhe.get("situacao", ""),
        "data_averbacao":  (detalhe.get("dataDaAverbacao")
                            or detalhe.get("dataDoDesembaraco", "")),
        "canal_due":       detalhe.get("canal", ""),
    }


# Mapeamento de códigos inteiros de situação da DUEResumida para texto legível
_SITUACOES_DUE_RESUMIDA = {
    1:  "EM_ELABORACAO",
    10: "REGISTRADA",
    11: "CARGA_APRESENTADA_PARA_DESPACHO",
    15: "ACD_EM_PROCESSAMENTO",
    20: "LIBERADA_SEM_CONFERENCIA",
    21: "SELECIONADA_PARA_CONFERENCIA",
    24: "EMBARQUE_ANTECIPADO_PENDENTE_LPCO",
    25: "EMBARQUE_ANTECIPADO_AUTORIZADO",
    26: "EMBARQUE_ANTECIPADO_PENDENTE_AUTORIZACAO",
    30: "EM_ANALISE_FISCAL",
    35: "CONCLUIDA_ANALISE_FISCAL",
    36: "DESEMBARACO_PENDENTE_LPCO",
    40: "DESEMBARACADA",
    70: "AVERBADA",
    80: "CANCELADA_PELO_EXPORTADOR",
    81: "CANCELADA_POR_EXPIRACAO",
    82: "CANCELADA_PELA_RFB",
    83: "CANCELADA_PELA_RFB_A_PEDIDO",
    86: "INTERROMPIDA",
}


def _extrair_campos_due_resumida(detalhe: dict) -> dict:
    """
    Parseia DUEResumida retornada pelo endpoint consultarDadosResumidosDUE.
    Estrutura completamente diferente da DUE completa.
    """
    exportadores = detalhe.get("exportadores") or []
    exportador_cnpj = exportadores[0].get("numero", "") if exportadores else ""

    sit_int = detalhe.get("situacaoDUE")
    situacao = _SITUACOES_DUE_RESUMIDA.get(sit_int, str(sit_int) if sit_int is not None else "")

    return {
        "exportador_cnpj": exportador_cnpj,
        "exportador_nome": "",
        "produto_ncm":     "",
        "produto_desc":    "",
        "peso_liquido_kg": None,
        "valor_fob_usd":   None,
        "pais_destino":    "",
        "porto_embarque":  "",
        "situacao_due":    situacao,
        "data_averbacao":  detalhe.get("dataSituacaoDUE", ""),
        "canal_due":       "",
    }


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
