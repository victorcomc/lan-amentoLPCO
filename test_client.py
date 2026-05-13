"""
Script de teste manual — rode diretamente para validar certificado e conexão.
Não faz parte do sistema de produção.

Uso:
    python test_client.py
"""

import logging
import sys

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)

from config import config
from siscomex_client import SiscomexClient


def main() -> None:
    print("=== Teste de Conexão Siscomex ===\n")

    # 1. Valida variáveis de ambiente
    try:
        config.validate()
        print("[OK] Configuração carregada.")
        print(f"     CNPJ:        {config.SISCOMEX_CNPJ}")
        print(f"     Base URL:    {config.SISCOMEX_BASE_URL}")
        print(f"     Certificado: {config.CERT_PFX_PATH}\n")
    except (ValueError, FileNotFoundError) as exc:
        print(f"[ERRO] {exc}")
        sys.exit(1)

    # 2. Testa carga do certificado e conectividade
    try:
        with SiscomexClient() as client:
            print("[OK] Certificado carregado e sessão mTLS iniciada.")

            print("\nTestando health-check...")
            online = client.verificar_conectividade()
            if online:
                print("[OK] API respondeu com sucesso.\n")
            else:
                print("[AVISO] Health-check retornou falha — verifique o endpoint.\n")

            print("Autenticando com Role-Type=IMPEXP...")
            autenticado = client.autenticar(config.WEBHOOK_ROLE_TYPE or "IMPEXP")
            if not autenticado:
                print("[ERRO] Autenticação falhou — verifique WEBHOOK_ROLE_TYPE e o certificado.")
                sys.exit(1)
            print("[OK] Autenticado com sucesso.\n")

            print("Executando consulta de LPCOs...")
            resultado = client.buscar_lpcos()

            if resultado.sucesso:
                print(f"[OK] Consulta bem-sucedida em {resultado.timestamp}.")
                print(f"     {len(resultado.registros)} LPCO(s) retornado(s).\n")
                for lpco in resultado.registros[:5]:  # mostra até 5
                    print(f"     • {lpco.numero} | {lpco.tipo} | {lpco.situacao}")
            else:
                print(f"[ERRO] Consulta falhou: {resultado.erro}")

    except ValueError as exc:
        print(f"[ERRO] Falha ao carregar certificado: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
