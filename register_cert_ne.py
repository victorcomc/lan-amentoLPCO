"""
Registra a subscrição webhook para o certificado nordeste (Felipe).
Executar UMA VEZ quando o cert do Felipe estiver disponível.

Uso:
    python register_cert_ne.py <caminho_do_pfx> <senha_do_pfx>

Exemplo:
    python register_cert_ne.py certs/felipe.pfx MinhaSenh@123

Após executar com sucesso:
  1. Anote o CPF do titular do cert (exibido ao final)
  2. Adicione no Coolify: CERT_NE_OWNER_ID=<cpf_do_felipe>
  3. Redeploy
"""

import sys
import logging
import os

from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def main() -> None:
    if len(sys.argv) != 3:
        print(__doc__)
        sys.exit(1)

    pfx_path = sys.argv[1]
    pfx_password = sys.argv[2]

    if not os.path.exists(pfx_path):
        logger.error("Arquivo não encontrado: %s", pfx_path)
        sys.exit(1)

    from cryptography.hazmat.primitives.serialization import pkcs12
    from cryptography.x509.oid import NameOID

    with open(pfx_path, "rb") as f:
        pfx_data = f.read()

    try:
        private_key, certificate, _ = pkcs12.load_key_and_certificates(
            pfx_data, pfx_password.encode("utf-8")
        )
    except Exception as exc:
        logger.error("Falha ao abrir o certificado: %s", exc)
        sys.exit(1)

    # Extrai o CPF/CNPJ do Subject do certificado (campo CN ou serialNumber)
    subject = certificate.subject
    owner_id = ""
    for attr in subject:
        if attr.oid == NameOID.SERIAL_NUMBER or attr.oid == NameOID.COMMON_NAME:
            val = attr.value
            # CPF/CNPJ aparece como sequência de dígitos no serialNumber
            digits = "".join(c for c in val if c.isdigit())
            if len(digits) in (11, 14):
                owner_id = digits
                break

    logger.info("Certificado carregado. Titular identificado: %s", owner_id or "(não detectado automaticamente)")

    # Sobrescreve temporariamente as vars do cert para usar o de Felipe
    os.environ["CERT_PFX_PATH"] = pfx_path
    os.environ["CERT_PFX_PASSWORD"] = pfx_password
    os.environ["CERT_PFX_BASE64"] = ""  # garante que usa o arquivo

    # Reimporta config com os novos valores
    import importlib
    import config as cfg_module
    importlib.reload(cfg_module)
    from config import config

    from webhook_manager import WebhookManager, EVENTO_LPCO
    from siscomex_client import SiscomexClient

    webhook_url = config.WEBHOOK_PUBLIC_URL
    logger.info("Registrando subscrição para: %s", webhook_url)

    mgr = WebhookManager(url_publica=webhook_url)
    ok = mgr.garantir_subscricao_ativa()

    if ok:
        logger.info("Subscrição registrada com sucesso!")
        if owner_id:
            print()
            print("=" * 60)
            print(f"  CERT_NE_OWNER_ID={owner_id}")
            print("=" * 60)
            print("Adicione essa variável no Coolify e faça redeploy.")
        else:
            print()
            print("CPF/CNPJ não detectado automaticamente.")
            print("Verifique o CN do certificado e preencha CERT_NE_OWNER_ID manualmente.")
    else:
        logger.error("Falha ao registrar. Verifique as credenciais e o log acima.")
        sys.exit(1)


if __name__ == "__main__":
    main()
