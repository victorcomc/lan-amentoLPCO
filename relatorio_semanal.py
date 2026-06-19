"""
Relatório semanal de LPCOs — módulo independente, não altera o sistema de webhooks.

Fluxo:
  1. Pagina GET /talpco/api/ext/lpco/consulta com o certificado SE até esgotar resultados
  2. Repete com o certificado NE, complementando os LPCOs que SE não retornou
  3. Para cada LPCO encontrado chama detalhar_lpco() para obter todos os campos disponíveis
  4. Gera Excel com duas abas:
       "Resumo por Cliente"  — totais agrupados por CNPJ/empresa
       "Detalhe LPCO"        — um LPCO por linha com todos os campos disponíveis
  5. Envia o arquivo por email como anexo

Agendamento padrão: toda segunda-feira às 08:00 (configurável em main.py).
"""

import logging
import time
from collections import defaultdict
from datetime import datetime, timedelta
from io import BytesIO
from typing import Any

from config import config
from siscomex_client import SiscomexClient

logger = logging.getLogger(__name__)

# Pausa entre chamadas à API para não sobrecarregar o servidor do Portal Único
_DELAY_ENTRE_CHAMADAS = 0.4  # segundos
_TAMANHO_PAGINA = 50


# ---------------------------------------------------------------------------
# Descoberta e detalhe via API (sem filtro de banco)
# ---------------------------------------------------------------------------

def _paginar_lpcos(client: SiscomexClient, label: str) -> list[str]:
    """
    Pagina o endpoint /consulta autenticado até esgotar os resultados.
    Retorna lista de números de LPCO encontrados.
    """
    numeros: list[str] = []
    pagina = 1

    while True:
        resultado = client.buscar_lpcos(pagina=pagina, tamanho=_TAMANHO_PAGINA)
        if not resultado.sucesso or not resultado.registros:
            if pagina == 1 and not resultado.sucesso:
                logger.warning("Falha na paginação %s: %s", label, resultado.erro)
            break

        for r in resultado.registros:
            if r.numero:
                numeros.append(r.numero)

        logger.info(
            "%s: página %d — %d LPCOs (total acumulado: %d)",
            label, pagina, len(resultado.registros), len(numeros),
        )

        if len(resultado.registros) < _TAMANHO_PAGINA:
            break  # última página

        pagina += 1
        time.sleep(_DELAY_ENTRE_CHAMADAS)

    return numeros


def _detalhar_em_sessao(
    numeros: list[str],
    pfx_path: str,
    pfx_base64: str,
    pfx_password: str,
    label: str,
) -> dict[str, dict]:
    """
    Autentica com um certificado, pagina /consulta para descobrir TODOS os LPCOs
    acessíveis e detalha cada um. Retorna {numero: raw_dict}.
    """
    resultado: dict[str, dict] = {}
    if not (pfx_path or pfx_base64):
        return resultado

    try:
        with SiscomexClient(
            cert_pfx_path=pfx_path,
            cert_pfx_base64=pfx_base64,
            cert_pfx_password=pfx_password,
        ) as client:
            if not client.autenticar(config.WEBHOOK_ROLE_TYPE):
                logger.error("Autenticação %s falhou para relatório semanal.", label)
                return resultado

            # Descobre quais LPCOs este certificado enxerga
            encontrados = _paginar_lpcos(client, label)

            # Remove os que já temos dados (passados como "numeros" = já detalhados por outro cert)
            pendentes = [n for n in encontrados if n not in numeros]
            logger.info(
                "%s: %d LPCOs encontrados, %d novos para detalhar.",
                label, len(encontrados), len(pendentes),
            )

            for i, numero in enumerate(pendentes, start=1):
                try:
                    raw = client.detalhar_lpco(numero)
                    resultado[numero] = raw if isinstance(raw, dict) else {}
                except Exception as exc:
                    logger.debug("%s: detalhe de %s — %s", label, numero, exc)
                    resultado[numero] = {}

                if i % 50 == 0:
                    logger.info("  %s: %d/%d detalhes obtidos.", label, i, len(pendentes))

                time.sleep(_DELAY_ENTRE_CHAMADAS)

    except Exception as exc:
        logger.error("Erro na sessão %s: %s", label, exc)

    return resultado


def _buscar_todos_detalhes() -> dict[str, dict]:
    """
    Descoberta completa: usa certificado SE e NE para varrer todos os LPCOs
    acessíveis a partir dos dois certificados (sem filtro do banco de dados).
    Retorna {numero_lpco: raw_dict_detalhe}.
    """
    # SE — descobre e detalha todos
    detalhes = _detalhar_em_sessao(
        numeros=[],           # nenhum pré-conhecido → detalha tudo que encontrar
        pfx_path=config.CERT_PFX_PATH,
        pfx_base64=config.CERT_PFX_BASE64,
        pfx_password=config.CERT_PFX_PASSWORD,
        label="SE",
    )

    # NE — complementa com LPCOs que SE não enxergou
    if config.CERT_NE_PFX_BASE64 or config.CERT_NE_PFX_PATH:
        ja_temos = list(detalhes.keys())
        ne = _detalhar_em_sessao(
            numeros=ja_temos,     # evita redetalhar o que SE já trouxe
            pfx_path=config.CERT_NE_PFX_PATH,
            pfx_base64=config.CERT_NE_PFX_BASE64,
            pfx_password=config.CERT_NE_PFX_PASSWORD,
            label="NE",
        )
        detalhes.update(ne)
    else:
        logger.info("Certificado NE não configurado — relatório apenas com dados SE.")

    return detalhes


# ---------------------------------------------------------------------------
# Parsing do response bruto
# ---------------------------------------------------------------------------

def _get_nested(raw: dict, *chaves: str) -> Any:
    """Tenta múltiplas chaves (suporta notação 'pai.filho'). Retorna o primeiro não-vazio."""
    for chave in chaves:
        v: Any = raw
        for parte in chave.split("."):
            if not isinstance(v, dict):
                v = None
                break
            v = v.get(parte)
        if v is not None and v != "":
            return v
    return ""


def _extrair_campos(numero: str, raw: dict) -> dict:
    """
    Extrai campos relevantes do response bruto do detalhe do LPCO.
    Usa múltiplos nomes de campo para acomodar variações da API.
    """
    requerente = (
        raw.get("requerente") or raw.get("importador") or
        raw.get("exportador") or raw.get("solicitante") or {}
    )
    mercadoria = (
        raw.get("mercadoria") or raw.get("produto") or
        raw.get("item") or raw.get("dadosMercadoria") or {}
    )
    situacao_obj = raw.get("situacao") if isinstance(raw.get("situacao"), dict) else {}
    pais_obj     = raw.get("paisDestino") or raw.get("pais") or raw.get("paisDeDestino") or {}
    porto_obj    = raw.get("portoEmbarque") or raw.get("porto") or raw.get("localEmbarque") or {}

    cnpj = (
        _get_nested(requerente, "cpfCnpj", "cnpj", "cpf") or
        _get_nested(raw, "cpfCnpj", "cnpj")
    )
    nome = (
        _get_nested(requerente, "nome", "razaoSocial", "nomeEmpresa") or
        _get_nested(raw, "nomeRequerente", "nomeEmpresa")
    )

    quantidade_raw = (
        _get_nested(mercadoria, "quantidadeAutorizada", "quantidade", "qtd") or
        _get_nested(raw, "quantidadeAutorizada", "quantidade")
    )
    try:
        quantidade = float(quantidade_raw) if quantidade_raw else 0.0
    except (ValueError, TypeError):
        quantidade = 0.0

    sit_id   = (
        _get_nested(situacao_obj, "id", "codigo") or
        (raw.get("situacao") if isinstance(raw.get("situacao"), str) else "") or ""
    )
    sit_desc = _get_nested(situacao_obj, "descricao", "nome") or ""

    pais_desc  = _get_nested(pais_obj, "descricao", "nome") or (pais_obj if isinstance(pais_obj, str) else "")
    porto_desc = _get_nested(porto_obj, "descricao", "nome") or (porto_obj if isinstance(porto_obj, str) else "")

    return {
        "numero_lpco":       numero,
        "cnpj":              cnpj,
        "nome_empresa":      nome,
        "codigo_modelo":     _get_nested(raw, "codigoModelo", "modelo", "tipoLpco"),
        "ncm":               _get_nested(mercadoria, "ncm", "codigoNcm") or _get_nested(raw, "ncm", "codigoNcm"),
        "descricao_produto": _get_nested(mercadoria, "descricao", "descricaoNcm", "nome") or _get_nested(raw, "descricaoProduto"),
        "quantidade":        quantidade,
        "unidade":           _get_nested(mercadoria, "unidade", "siglaUnidade") or _get_nested(raw, "unidade"),
        "pais_destino":      pais_desc,
        "porto_embarque":    porto_desc,
        "embarcacao":        _get_nested(raw, "embarcacao", "nomeEmbarcacao", "navio"),
        "data_validade":     _get_nested(raw, "dataValidade", "validade", "dtValidade"),
        "situacao_id":       sit_id.upper() if sit_id else "",
        "situacao_desc":     sit_desc,
    }


# ---------------------------------------------------------------------------
# Geração do Excel
# ---------------------------------------------------------------------------

_CORES_SITUACAO = {
    "DEFERIDO":         "C6EFCE",
    "INDEFERIDO":       "FFC7CE",
    "EM_ANALISE":       "FFEB9C",
    "EM_VERIFICACAO":   "DDEBF7",
    "CANCELADO":        "D9D9D9",
}


def _aplicar_cabecalho(ws: Any, titulos: list[str], cor_hex: str = "1F4E79") -> None:
    from openpyxl.styles import Font, PatternFill, Alignment
    ws.append(titulos)
    fill  = PatternFill("solid", fgColor=cor_hex)
    fonte = Font(bold=True, color="FFFFFF", size=11)
    for cell in ws[1]:
        cell.fill  = fill
        cell.font  = fonte
        cell.alignment = Alignment(horizontal="center", wrap_text=True)


def _ajustar_colunas(ws: Any) -> None:
    from openpyxl.utils import get_column_letter
    for col in ws.columns:
        largura = max((len(str(c.value or "")) for c in col), default=10)
        ws.column_dimensions[get_column_letter(col[0].column)].width = min(largura + 4, 55)


def _gerar_excel(dados: list[dict], periodo_label: str) -> bytes:
    try:
        import openpyxl
        from openpyxl.styles import PatternFill
    except ImportError:
        raise RuntimeError("openpyxl não instalado — execute: pip install openpyxl")

    wb = openpyxl.Workbook()

    # -----------------------------------------------------------------------
    # Aba 1 — Resumo por Cliente
    # -----------------------------------------------------------------------
    ws1 = wb.active
    ws1.title = "Resumo por Cliente"

    clientes: dict[str, dict] = defaultdict(lambda: {
        "nome": "",
        "lpcos": set(),
        "total_kg": 0.0,
        "unidades": set(),
        "produtos": set(),
        "paises": set(),
        "portos": set(),
        "embarcacoes": set(),
        "situacoes": defaultdict(int),
    })

    for d in dados:
        chave = d["cnpj"] or "N/I"
        c = clientes[chave]
        if d["nome_empresa"]:
            c["nome"] = d["nome_empresa"]
        c["lpcos"].add(d["numero_lpco"])
        c["total_kg"] += d["quantidade"]
        for campo, dest in [
            ("unidade", "unidades"),
            ("descricao_produto", "produtos"),
            ("pais_destino", "paises"),
            ("porto_embarque", "portos"),
            ("embarcacao", "embarcacoes"),
        ]:
            if d[campo]:
                c[dest].add(d[campo])
        c["situacoes"][d["situacao_id"] or "SEM_INFO"] += 1

    _aplicar_cabecalho(ws1, [
        "CNPJ / CPF", "Nome Empresa", "Total LPCOs",
        "Qtd. Total Autorizada", "Unidade",
        "Produtos / Espécies", "Países Destino",
        "Portos Embarque", "Embarcações", "Situações",
    ])

    for cnpj, c in sorted(clientes.items(), key=lambda x: -len(x[1]["lpcos"])):
        sits_str = "   ".join(f"{k}: {v}" for k, v in sorted(c["situacoes"].items()))
        ws1.append([
            cnpj,
            c["nome"],
            len(c["lpcos"]),
            round(c["total_kg"], 3) if c["total_kg"] else "",
            " | ".join(sorted(c["unidades"])),
            " | ".join(sorted(c["produtos"])),
            " | ".join(sorted(c["paises"])),
            " | ".join(sorted(c["portos"])),
            " | ".join(sorted(c["embarcacoes"])),
            sits_str,
        ])

    _ajustar_colunas(ws1)
    ws1.freeze_panes = "A2"
    ws1.auto_filter.ref = ws1.dimensions

    # -----------------------------------------------------------------------
    # Aba 2 — Detalhe por LPCO
    # -----------------------------------------------------------------------
    ws2 = wb.create_sheet("Detalhe LPCO")

    _aplicar_cabecalho(ws2, [
        "Número LPCO", "CNPJ / CPF", "Nome Empresa",
        "Código Modelo", "NCM", "Produto / Espécie",
        "Qtd. Autorizada", "Unidade",
        "País Destino", "Porto Embarque", "Embarcação",
        "Validade", "Situação",
    ])

    for d in sorted(dados, key=lambda x: (x.get("cnpj", ""), x.get("numero_lpco", ""))):
        sit_label = d["situacao_id"]
        if d["situacao_desc"]:
            sit_label = f"{d['situacao_id']} — {d['situacao_desc']}"

        ws2.append([
            d["numero_lpco"], d["cnpj"], d["nome_empresa"],
            d["codigo_modelo"], d["ncm"], d["descricao_produto"],
            d["quantidade"] if d["quantidade"] else "", d["unidade"],
            d["pais_destino"], d["porto_embarque"], d["embarcacao"],
            d["data_validade"], sit_label,
        ])

        cor = _CORES_SITUACAO.get(d["situacao_id"].upper() if d["situacao_id"] else "", "")
        if cor:
            fill = PatternFill("solid", fgColor=cor)
            for cell in ws2[ws2.max_row]:
                cell.fill = fill

    _ajustar_colunas(ws2)
    ws2.freeze_panes = "A2"
    ws2.auto_filter.ref = ws2.dimensions

    # -----------------------------------------------------------------------
    # Aba 3 — Legenda
    # -----------------------------------------------------------------------
    ws3 = wb.create_sheet("Legenda")
    from openpyxl.styles import Font, PatternFill
    ws3.append(["Cor", "Situação", "Descrição"])
    for cell in ws3[1]:
        cell.font = Font(bold=True)
    legenda = [
        ("C6EFCE", "DEFERIDO",       "LPCO aprovado pelo órgão anuente"),
        ("FFC7CE", "INDEFERIDO",      "LPCO negado pelo órgão anuente"),
        ("FFEB9C", "EM_ANALISE",      "Em análise pelo órgão anuente"),
        ("DDEBF7", "EM_VERIFICACAO",  "Aguardando verificação/documentação"),
        ("D9D9D9", "CANCELADO",       "LPCO cancelado"),
        ("FFFFFF", "Demais",          "Outras situações sem cor específica"),
    ]
    for cor, sit, desc in legenda:
        ws3.append(["", sit, desc])
        ws3.cell(ws3.max_row, 1).fill = PatternFill("solid", fgColor=cor)
    from openpyxl.utils import get_column_letter
    for i, larg in enumerate([8, 25, 45], start=1):
        ws3.column_dimensions[get_column_letter(i)].width = larg

    wb.properties.description = f"Relatório LPCO Hevile — {periodo_label}"
    buf = BytesIO()
    wb.save(buf)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Ponto de entrada do job semanal
# ---------------------------------------------------------------------------

def gerar_e_enviar_relatorio_semanal() -> None:
    """
    Varre TODOS os LPCOs acessíveis via certificados SE e NE (sem filtro do banco),
    gera Excel e envia por email. Chamado pelo APScheduler toda segunda às 08:00.
    """
    from email_service import enviar_relatorio_excel

    agora    = datetime.now()
    inicio   = agora - timedelta(days=7)
    periodo  = f"{inicio.strftime('%d/%m/%Y')} a {agora.strftime('%d/%m/%Y')}"
    nome_arq = f"relatorio_lpco_{agora.strftime('%Y-%m-%d')}.xlsx"

    logger.info("=== Relatório semanal iniciando — período: %s ===", periodo)

    # 1. Descobre e detalha TODOS os LPCOs via API (SE + NE)
    detalhes = _buscar_todos_detalhes()

    if not detalhes:
        logger.error("Nenhum LPCO retornado pela API — relatório cancelado.")
        return

    com_dados = sum(1 for v in detalhes.values() if v)
    logger.info(
        "Total de LPCOs encontrados: %d (%d com detalhe completo).",
        len(detalhes), com_dados,
    )

    # 2. Extrai campos para o Excel
    dados = [_extrair_campos(num, raw) for num, raw in detalhes.items()]

    # 3. Gera Excel
    try:
        excel_bytes = _gerar_excel(dados, periodo)
    except Exception as exc:
        logger.error("Erro ao gerar Excel do relatório semanal: %s", exc)
        return

    # 4. Envia por email
    try:
        enviar_relatorio_excel(excel_bytes, nome_arq, periodo)
        logger.info("=== Relatório semanal enviado: %s ===", nome_arq)
    except Exception as exc:
        logger.error("Erro ao enviar relatório semanal por email: %s", exc)
