"""
Script de descoberta da API de DUE no Portal Único Siscomex.

Roda UMA VEZ no servidor para mapear os endpoints disponíveis e os campos
que o certificado do Diogenes consegue acessar.

Uso:
    python test_due.py

Resultado: imprime status HTTP + JSON de cada tentativa de endpoint.
"""

import json
import logging
import sys

logging.basicConfig(level=logging.INFO, format="%(levelname)s — %(message)s")
logger = logging.getLogger(__name__)

DUE_NUMERO = "26BR0010956873"
RUC_NUMERO = "6BR26244457200000000000000001124340"

# Role-Types a testar
ROLE_TYPES = ["IMPEXP", "REPR", "TRANSPORT", "DEPOSIT", "OPERPORT", "EXPORT", "EXPBR", "DESPACHANTE"]

# Endpoints que retornaram 405 (existem mas precisam de POST)
ENDPOINTS_POST = [
    ("/due/api/ext/due/consulta",        {"numero": DUE_NUMERO}),
    ("/due/api/ext/due/consulta",        {"numeroDUE": DUE_NUMERO}),
    ("/due/api/ext/due/consulta",        {"due": DUE_NUMERO}),
    ("/due/api/ext/due/consulta",        {"numeroDue": DUE_NUMERO}),
    (f"/due/api/ext/due/{DUE_NUMERO}",   {}),
]

# GET — caminhos adicionais a tentar
ENDPOINTS_GET_DUE = [
    f"/due/api/ext/due/{DUE_NUMERO}",
    f"/due/api/ext/due?numero={DUE_NUMERO}",
    f"/due/api/ext/due?numeroDUE={DUE_NUMERO}",
    f"/due/api/ext/due/{DUE_NUMERO}/itens",
    f"/due/api/ext/due/{DUE_NUMERO}/declarante",
    f"/due/api/ext/due/{DUE_NUMERO}/exportador",
    f"/due/api/ext/due/{DUE_NUMERO}/frete",
    f"/due/api/ext/due/{DUE_NUMERO}/vmle",          # Valor da Mercadoria no Local de Embarque
    f"/exportacao/api/ext/due/{DUE_NUMERO}",
    f"/exportacao/api/ext/due/consulta",
    f"/exp/api/ext/due/{DUE_NUMERO}",
]

ENDPOINTS_CE = [
    f"/ce-mercante/api/ext/conhecimento/{RUC_NUMERO}",
    f"/ce-mercante/api/ext/ruc/{RUC_NUMERO}",
    f"/ce-mercante/api/ext/carga/{RUC_NUMERO}",
    f"/ce-mercante/api/ext/conhecimento?ruc={RUC_NUMERO}",
]


def _tentar(session, base_url: str, path: str, label: str = "") -> dict | None:
    import requests
    url = f"{base_url}{path}"
    try:
        resp = session.get(url, timeout=(15, 30))
        status = resp.status_code
        body = ""
        try:
            body = resp.json()
        except Exception:
            body = resp.text[:500]

        marker = "✓" if status == 200 else ("⚠" if status in (401, 403) else "✗")
        logger.info("%s [%d] %s", marker, status, path)

        if status == 200:
            logger.info("    RESPOSTA:\n%s", json.dumps(body, ensure_ascii=False, indent=2)[:3000])
            return body
        elif status in (400, 422):
            logger.info("    (endpoint existe, parâmetro inválido): %s", str(body)[:200])
        elif status == 404:
            pass  # endpoint não existe
        elif status in (401, 403):
            logger.info("    (acesso negado — Role-Type pode ser diferente)")
        return None
    except Exception as exc:
        logger.debug("  ERRO em %s: %s", path, exc)
        return None


def main() -> None:
    from config import config
    from siscomex_client import SiscomexClient

    base_url = config.SISCOMEX_BASE_URL.rstrip("/")
    logger.info("Base URL: %s", base_url)
    logger.info("DUE testada: %s", DUE_NUMERO)
    logger.info("RUC testado: %s", RUC_NUMERO)
    logger.info("=" * 60)

    # -----------------------------------------------------------------------
    # Testa autenticação com diferentes Role-Types
    # -----------------------------------------------------------------------
    role_ok = None
    session_obj = None

    for role in ROLE_TYPES:
        logger.info("\nTentando Role-Type: %s", role)
        try:
            client = SiscomexClient().__enter__()
            ok = client.autenticar(role)
            if ok:
                logger.info("  ✓ Autenticação bem-sucedida com Role-Type=%s", role)
                role_ok = role
                session_obj = client
                break
            else:
                client.__exit__(None, None, None)
        except Exception as exc:
            logger.warning("  Erro ao testar Role-Type=%s: %s", role, exc)

    if not session_obj:
        logger.error("Nenhum Role-Type funcionou. Verifique o certificado e a conectividade.")
        sys.exit(1)

    sess = session_obj._session  # type: ignore[attr-defined]

    # -----------------------------------------------------------------------
    # POST nos endpoints que retornaram 405
    # -----------------------------------------------------------------------
    logger.info("\n%s\nTESTANDO POST NOS ENDPOINTS 405\n%s", "=" * 60, "=" * 60)
    achados_due: dict = {}
    import requests as _req
    for path, body in ENDPOINTS_POST:
        url = f"{base_url}{path}"
        try:
            resp = sess.post(url, json=body, timeout=(15, 30))
            marker = "✓" if resp.status_code == 200 else ("⚠" if resp.status_code in (400, 422) else "✗")
            logger.info("%s [%d] POST %s  body=%s", marker, resp.status_code, path, body)
            if resp.status_code == 200:
                data = resp.json()
                logger.info("    RESPOSTA:\n%s", json.dumps(data, ensure_ascii=False, indent=2)[:3000])
                achados_due[f"POST {path}"] = data
            elif resp.status_code in (400, 422):
                logger.info("    (endpoint ativo, erro de parâmetro): %s", resp.text[:300])
        except Exception as exc:
            logger.debug("  ERRO: %s", exc)

    # -----------------------------------------------------------------------
    # GET — mais variações de endpoint DUE
    # -----------------------------------------------------------------------
    logger.info("\n%s\nTESTANDO GET ENDPOINTS DUE\n%s", "=" * 60, "=" * 60)
    for path in ENDPOINTS_GET_DUE:
        resultado = _tentar(sess, base_url, path)
        if resultado is not None:
            achados_due[f"GET {path}"] = resultado

    # -----------------------------------------------------------------------
    # Testa Role-Types alternativos nos endpoints que deram 403
    # -----------------------------------------------------------------------
    logger.info("\n%s\nTESTANDO ROLE-TYPES ALTERNATIVOS (403s)\n%s", "=" * 60, "=" * 60)
    paths_403 = [
        f"/exportacao/api/ext/due/{DUE_NUMERO}",
        f"/exportacao/api/ext/due/consulta",
        f"/ce-mercante/api/ext/conhecimento/{RUC_NUMERO}",
        f"/ce-mercante/api/ext/ruc/{RUC_NUMERO}",
    ]
    session_obj.__exit__(None, None, None)  # encerra sessão IMPEXP

    for role in ROLE_TYPES[1:]:  # pula IMPEXP (já testado)
        logger.info("\n--- Role-Type: %s ---", role)
        try:
            client2 = SiscomexClient().__enter__()
            ok2 = client2.autenticar(role)
            if not ok2:
                logger.info("  Autenticação negada.")
                client2.__exit__(None, None, None)
                continue
            logger.info("  ✓ Autenticado com Role-Type=%s", role)
            sess2 = client2._session  # type: ignore[attr-defined]
            for path in paths_403:
                resultado = _tentar(sess2, base_url, path, role)
                if resultado is not None:
                    achados_due[f"GET {path} [{role}]"] = resultado
            client2.__exit__(None, None, None)
        except Exception as exc:
            logger.warning("  Erro com Role-Type=%s: %s", role, exc)

    # -----------------------------------------------------------------------
    # CE-Mercante com IMPEXP (nova sessão)
    # -----------------------------------------------------------------------
    logger.info("\n%s\nTESTANDO CE-MERCANTE\n%s", "=" * 60, "=" * 60)
    achados_ce: dict = {}
    try:
        client3 = SiscomexClient().__enter__()
        if client3.autenticar("IMPEXP"):
            sess3 = client3._session  # type: ignore[attr-defined]
            for path in ENDPOINTS_CE:
                resultado = _tentar(sess3, base_url, path)
                if resultado is not None:
                    achados_ce[path] = resultado
        client3.__exit__(None, None, None)
    except Exception as exc:
        logger.error("Erro CE-Mercante: %s", exc)

    session_obj.__exit__(None, None, None)

    # -----------------------------------------------------------------------
    # Resumo
    # -----------------------------------------------------------------------
    logger.info("\n%s\nRESUMO\n%s", "=" * 60, "=" * 60)
    logger.info("Role-Type que funcionou: %s", role_ok)
    logger.info("Endpoints DUE com resposta 200: %d", len(achados_due))
    for p in achados_due:
        logger.info("  ✓ %s", p)
    logger.info("Endpoints CE-Mercante com resposta 200: %d", len(achados_ce))
    for p in achados_ce:
        logger.info("  ✓ %s", p)

    if not achados_due and not achados_ce:
        logger.warning(
            "\nNenhum endpoint respondeu 200. Possíveis causas:\n"
            "  1. Os caminhos da API de DUE são diferentes dos testados\n"
            "  2. O certificado precisa de Role-Type específico para DUE\n"
            "  3. A Hevile não está cadastrada como despachante para essa DUE\n"
            "Me mande o log completo para analisar os status codes."
        )


if __name__ == "__main__":
    main()
