import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    # Certificado A1 — duas formas de fornecer (use uma delas):
    #   Arquivo local:  CERT_PFX_PATH + CERT_PFX_PASSWORD
    #   Produção/Docker: CERT_PFX_BASE64 + CERT_PFX_PASSWORD
    CERT_PFX_PATH: str = os.getenv("CERT_PFX_PATH", "")
    CERT_PFX_BASE64: str = os.getenv("CERT_PFX_BASE64", "")
    CERT_PFX_PASSWORD: str = os.getenv("CERT_PFX_PASSWORD", "")

    # API Siscomex
    SISCOMEX_BASE_URL: str = os.getenv(
        "SISCOMEX_BASE_URL", "https://portalunico.siscomex.gov.br"
    )
    SISCOMEX_CNPJ: str = os.getenv("SISCOMEX_CNPJ", "")

    # SMTP Outlook
    SMTP_HOST: str = os.getenv("SMTP_HOST", "smtp.office365.com")
    SMTP_PORT: int = int(os.getenv("SMTP_PORT", "587"))
    SMTP_USER: str = os.getenv("SMTP_USER", "")
    SMTP_APP_PASSWORD: str = os.getenv("SMTP_APP_PASSWORD", "")

    # Destinatários — alertas de sistema (falhas, saúde do webhook)
    EMAIL_OPERACAO: str = os.getenv("EMAIL_OPERACAO", "")
    # Destinatários por tipo de LPCO
    EMAIL_FRUTA: str = os.getenv("EMAIL_FRUTA", "")   # modelo E00144
    EMAIL_PESCA: str = os.getenv("EMAIL_PESCA", "")   # modelo E00061 (sudeste e nordeste)
    # CPFs dos titulares dos certificados — usados para filtrar eventos que são de vocês
    CERT_SE_OWNER_ID: str = os.getenv("CERT_SE_OWNER_ID", "")  # Diogenes (sudeste)
    CERT_NE_OWNER_ID: str = os.getenv("CERT_NE_OWNER_ID", "")  # Felipe (nordeste)

    # Webhook receiver
    WEBHOOK_SECRET: str = os.getenv("WEBHOOK_SECRET", "")
    WEBHOOK_PORT: int = int(os.getenv("WEBHOOK_PORT", "8080"))
    WEBHOOK_ROLE_TYPE: str = os.getenv("WEBHOOK_ROLE_TYPE", "")
    WEBHOOK_PUBLIC_URL: str = os.getenv("WEBHOOK_PUBLIC_URL", "")

    # Verificação de saúde da subscrição
    WEBHOOK_HEALTH_CHECK_HOURS: int = int(os.getenv("WEBHOOK_HEALTH_CHECK_HOURS", "6"))

    def validate(self) -> None:
        required = {
            "CERT_PFX_PASSWORD": self.CERT_PFX_PASSWORD,
            "SISCOMEX_CNPJ": self.SISCOMEX_CNPJ,
            "SMTP_USER": self.SMTP_USER,
            "SMTP_APP_PASSWORD": self.SMTP_APP_PASSWORD,
            "EMAIL_OPERACAO": self.EMAIL_OPERACAO,
            "EMAIL_FRUTA": self.EMAIL_FRUTA,
            "EMAIL_PESCA": self.EMAIL_PESCA,
            "WEBHOOK_SECRET": self.WEBHOOK_SECRET,
            "WEBHOOK_ROLE_TYPE": self.WEBHOOK_ROLE_TYPE,
            "WEBHOOK_PUBLIC_URL": self.WEBHOOK_PUBLIC_URL,
        }
        missing = [k for k, v in required.items() if not v]
        if missing:
            raise ValueError(f"Variáveis de ambiente ausentes: {', '.join(missing)}")

        if not self.CERT_PFX_BASE64 and not self.CERT_PFX_PATH:
            raise ValueError(
                "Forneça o certificado via CERT_PFX_BASE64 (produção) "
                "ou CERT_PFX_PATH (local)."
            )

        if self.CERT_PFX_PATH and not os.path.exists(self.CERT_PFX_PATH):
            raise FileNotFoundError(
                f"Certificado não encontrado: {self.CERT_PFX_PATH}"
            )


config = Config()
