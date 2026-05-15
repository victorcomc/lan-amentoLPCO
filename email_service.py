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
