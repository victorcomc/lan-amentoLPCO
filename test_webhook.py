"""
Simula um evento webhook do Siscomex para testar o sistema em produção.
Uso: python test_webhook.py
"""

import requests
import json
import os
from dotenv import load_dotenv

load_dotenv()

URL    = "https://siscomex.hevile.com.br/webhook/lpco"
SECRET = os.getenv("WEBHOOK_SECRET", "")

if not SECRET:
    print("[ERRO] WEBHOOK_SECRET não encontrado no .env")
    exit(1)

payload = {
    "numeroLPCO": "E2600000001-TESTE",
    "codigoModelo": "E00061",
    "novaSituacao": {
        "id": "DEFERIDO",
        "descricao": "Deferido"
    },
    "justificativa": "Documentação aprovada — teste do sistema de monitoramento.",
    "cpfCnpj": ["02255486000134"],
    "dataEvento": "2026-05-13T18:00:00.000Z"
}

headers = {
    "Content-Type":   "application/json",
    "Secret":         SECRET,
    "event-type":     "talp-altsit-lpco-anu",
    "destinatario-id": "02255486000134",
}

print(f"Enviando evento de teste para {URL}...")
print(f"  LPCO: {payload['numeroLPCO']}")
print(f"  Situação: {payload['novaSituacao']['id']}")
print()

resp = requests.post(URL, json=payload, headers=headers, timeout=10)
print(f"Resposta: HTTP {resp.status_code}")

if resp.status_code == 200:
    print("[OK] Evento aceito pelo servidor.")
    print("     Verifique a caixa de entrada de victor.gabriele@hevile.com.br")
elif resp.status_code == 401:
    print("[ERRO] Secret inválido — confira WEBHOOK_SECRET no .env e no Coolify.")
else:
    print(f"[AVISO] Resposta inesperada: {resp.text[:200]}")
