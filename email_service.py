"""
Envio de emails via SMTP Outlook com senha de aplicativo.
"""

import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from config import config

logger = logging.getLogger(__name__)


def _send(subject: str, body_html: str, to: list[str]) -> None:
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = config.SMTP_USER
    msg["To"] = ", ".join(to)
    msg.attach(MIMEText(body_html, "html", "utf-8"))

    with smtplib.SMTP(config.SMTP_HOST, config.SMTP_PORT, timeout=30) as smtp:
        smtp.ehlo()
        smtp.starttls()
        smtp.login(config.SMTP_USER, config.SMTP_APP_PASSWORD)
        smtp.sendmail(config.SMTP_USER, to, msg.as_string())

    logger.info("Email enviado para %s | assunto: %s", to, subject)


def notificar_lpco_liberado(
    numero_lpco: str,
    situacao: str,
    detalhes: dict,
    destinatarios: list[str],
    tipo: str = "",
) -> None:
    """
    Dispara email quando um LPCO é deferido/aprovado.
    `detalhes` é o payload bruto recebido no webhook.
    `tipo` aparece no assunto: ex "Fruta", "Pesca SE", "Pesca NE".
    """
    tipo_suffix = f" ({tipo})" if tipo else ""
    corpo = f"""
    <html><body>
    <h2>LPCO {numero_lpco} — {situacao}{tipo_suffix}</h2>
    <p>O LPCO <strong>{numero_lpco}</strong> teve sua situação atualizada para
    <strong>{situacao}</strong>.</p>
    <h3>Dados do evento</h3>
    <pre style="background:#f4f4f4;padding:12px">{_fmt_dict(detalhes)}</pre>
    <p style="color:#888;font-size:12px">
        Sistema de monitoramento automático — Portal Único Siscomex
    </p>
    </body></html>
    """
    _send(
        subject=f"[LPCO] {numero_lpco} — {situacao}{tipo_suffix}",
        body_html=corpo,
        to=destinatarios,
    )


def notificar_falha_webhook(erro: str) -> None:
    """
    Alerta de operação: subscrição expirada, falha repetida ou evento não processado.
    """
    corpo = f"""
    <html><body>
    <h2>⚠ Alerta — Monitor LPCO</h2>
    <p><strong>Descrição:</strong> {erro}</p>
    <p>Verifique o log do sistema e a subscrição no Portal Único.</p>
    </body></html>
    """
    _send(
        subject="[ALERTA] Monitor LPCO — falha no webhook",
        body_html=corpo,
        to=[config.EMAIL_OPERACAO],
    )


def enviar_relatorio_excel(
    excel_bytes: bytes,
    nome_arquivo: str,
    periodo: str,
    destinatarios: list[str] | None = None,
    resumo_extra: str = "",
) -> None:
    """
    Envia o relatório semanal de LPCOs como anexo Excel.
    Se destinatarios não for informado, usa EMAIL_OPERACAO.
    resumo_extra: texto adicional incluído no corpo do email (stats, observações).
    """
    from email.mime.base import MIMEBase
    from email import encoders

    to = destinatarios or [config.EMAIL_OPERACAO]

    msg = MIMEMultipart("mixed")
    msg["Subject"] = f"[Relatório LPCO] Hevile — {periodo}"
    msg["From"]    = config.SMTP_USER
    msg["To"]      = ", ".join(to)

    resumo_html = ""
    if resumo_extra:
        linhas = resumo_extra.replace("\n", "<br>")
        resumo_html = f"""
    <div style="background:#f0f4ff;border-left:4px solid #1F4E79;padding:10px 16px;margin:12px 0;font-family:monospace;font-size:13px">
      {linhas}
    </div>"""

    corpo_html = f"""
    <html><body>
    <h2>Relatório Semanal de LPCOs — Hevile Logística</h2>
    <p>Segue em anexo o relatório do período <strong>{periodo}</strong>.</p>
    {resumo_html}
    <p>O arquivo Excel contém:</p>
    <ul>
      <li><strong>Visão Consolidada</strong> — LPCO e DUE cruzados por empresa/CNPJ</li>
      <li><strong>Resumo por Cliente</strong> — totais agrupados por CNPJ/empresa</li>
      <li><strong>Detalhe LPCO</strong> — todos os processos individualmente (coluna "Em SharePoint")</li>
      <li><strong>Resumo Mercado / Detalhe DUEs</strong> — eventos DUE recebidos via webhook</li>
      <li><strong>Análise de Mercado</strong> — rankings e totais gerais</li>
    </ul>
    <p style="color:#888;font-size:12px">
        Sistema de monitoramento automático — Portal Único Siscomex
    </p>
    </body></html>
    """
    msg.attach(MIMEText(corpo_html, "html", "utf-8"))

    parte = MIMEBase("application", "vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    parte.set_payload(excel_bytes)
    encoders.encode_base64(parte)
    parte.add_header("Content-Disposition", f'attachment; filename="{nome_arquivo}"')
    msg.attach(parte)

    with smtplib.SMTP(config.SMTP_HOST, config.SMTP_PORT, timeout=60) as smtp:
        smtp.ehlo()
        smtp.starttls()
        smtp.login(config.SMTP_USER, config.SMTP_APP_PASSWORD)
        smtp.sendmail(config.SMTP_USER, to, msg.as_string())

    logger.info("Relatório Excel enviado para %s | arquivo: %s", to, nome_arquivo)


def _fmt_dict(d: dict, indent: int = 0) -> str:
    lines = []
    for k, v in d.items():
        prefix = "  " * indent
        if isinstance(v, dict):
            lines.append(f"{prefix}{k}:")
            lines.append(_fmt_dict(v, indent + 1))
        else:
            lines.append(f"{prefix}{k}: {v}")
    return "\n".join(lines)
