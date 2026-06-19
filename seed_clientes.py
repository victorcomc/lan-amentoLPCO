"""
Popula a tabela clientes_cnpj com os CNPJs dos clientes da Hevile.

Rode UMA VEZ no servidor após o deploy:
    python seed_clientes.py

Pode ser executado novamente sem problemas (INSERT OR REPLACE).
"""

from database import registrar_cnpj_cliente, listar_cnpjs_clientes, init_db

CLIENTES = [
    "48.812.543/0001-30",
    "00.605.555/0001-67",
    "53.102.491/0001-48",
    "64.654.650/0001-33",
    "26.244.457/0001-35",
    "11.591.434/0001-20",
    "55.573.936/0001-01",
    "42.135.730/0001-40",
    "48.115.574/0001-31",
    "56.026.840/0001-88",
    "08.215.522/0001-12",
    "08.215.522/0005-46",
    "08.215.522/0006-27",
    "08.215.522/0007-08",
    "08.215.522/0009-70",
    "31.276.127/0001-61",
    "15.452.593/0011-76",
    "15.531.898/0001-00",
    "41.391.017/0001-02",
    "41.391.017/0002-85",
    "41.999.541/0001-52",
    "56.420.707/0001-01",
]

if __name__ == "__main__":
    init_db()
    inseridos = 0
    for cnpj in CLIENTES:
        if registrar_cnpj_cliente(cnpj):
            inseridos += 1
            print(f"  + {cnpj}")
        else:
            print(f"  = {cnpj} (já existia)")

    total = len(listar_cnpjs_clientes())
    print(f"\nConcluído: {inseridos} inseridos. Total ativo no banco: {total}")