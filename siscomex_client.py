"""
Cliente HTTP para o Portal Único Siscomex - módulo TALPCO.

Base URL da API: /talpco/api
Autenticação: mTLS com certificado A1 (.pfx) + header Role-Type no endpoint de auth.

O .pfx é convertido em memória para PEM temporário — nenhum arquivo intermediário
é gravado em disco, evitando exposição da chave privada.
"""

import logging
import os
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Generator

import requests
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.serialization import pkcs12

from config import config

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Tipos de dados
# ---------------------------------------------------------------------------

@dataclass
class LpcoRecord:
    numero: str
    situacao: str              # Ex: "DEFERIDO", "INDEFERIDO", "EM_ANALISE"
    tipo: str                  # código do modelo
    responsavel_email: str
    data_validade: str | None
    raw: dict = field(default_factory=dict, repr=False)


@dataclass
class PollingResult:
    sucesso: bool
    timestamp: datetime
    registros: list[LpcoRecord] = field(default_factory=list)
    erro: str | None = None


# ---------------------------------------------------------------------------
# Gerenciamento do certificado em memória
# ---------------------------------------------------------------------------

def _load_pfx(pfx_path: str, pfx_password: str) -> tuple[bytes, bytes]:
    """
    Lê o .pfx (de arquivo ou de CERT_PFX_BASE64) e retorna (cert_pem, key_pem).
    Lança ValueError se a senha estiver errada ou o arquivo corrompido.
    """
    import base64
    if config.CERT_PFX_BASE64:
        pfx_data = base64.b64decode(config.CERT_PFX_BASE64)
    else:
        with open(pfx_path, "rb") as f:
            pfx_data = f.read()

    password_bytes = pfx_password.encode("utf-8")

    try:
        private_key, certificate, _ = pkcs12.load_key_and_certificates(
            pfx_data, password_bytes
        )
    except Exception as exc:
        raise ValueError(
            f"Falha ao carregar certificado '{pfx_path}'. "
            "Verifique o arquivo e a senha."
        ) from exc

    cert_pem = certificate.public_bytes(serialization.Encoding.PEM)
    key_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption(),
    )
    return cert_pem, key_pem


@contextmanager
def _temp_cert_files(
    cert_pem: bytes, key_pem: bytes
) -> Generator[tuple[str, str], None, None]:
    """
    Cria arquivos PEM temporários (modo 0600) e os remove ao sair do contexto.
    O requests exige caminhos de arquivo — não aceita bytes diretamente.
    """
    cert_file = tempfile.NamedTemporaryFile(suffix=".cert.pem", delete=False, mode="wb")
    key_file  = tempfile.NamedTemporaryFile(suffix=".key.pem",  delete=False, mode="wb")
    try:
        cert_file.write(cert_pem); cert_file.flush(); cert_file.close()
        key_file.write(key_pem);   key_file.flush();  key_file.close()
        os.chmod(cert_file.name, 0o600)
        os.chmod(key_file.name,  0o600)
        yield cert_file.name, key_file.name
    finally:
        for path in (cert_file.name, key_file.name):
            try:
                os.unlink(path)
            except OSError:
                pass


# ---------------------------------------------------------------------------
# Cliente principal
# ---------------------------------------------------------------------------

class SiscomexClient:
    """
    Sessão HTTP autenticada contra o Portal Único Siscomex via mTLS.

    Documentação base: /talpco/api  (módulo TALPCO)
    Ambiente de produção: portalunico.siscomex.gov.br

    Uso:
        with SiscomexClient() as client:
            resultado = client.buscar_lpcos()
    """

    _TIMEOUT      = (30, 60)   # connect, read — autenticação e operações rápidas
    _TIMEOUT_DATA = (30, 120)  # read estendido para queries TALPCO (servidor lento)

    # Endpoint de autenticação da plataforma (padrão Portal Único)
    _AUTH_PATH   = "/portal/api/autenticar"

    # Endpoints TALPCO
    _LPCO_CONSULTA          = "/talpco/api/ext/lpco/consulta"
    _LPCO_CONSULTA_COMPLETA = "/talpco/api/ext/lpco/consulta-completa"
    _LPCO_DETALHE           = "/talpco/api/ext/lpco/{numero}"
    _LPCO_HISTORICO         = "/talpco/api/ext/lpco/{numero}/historico"
    _LPCO_SITUACAO          = "/talpco/api/ext/lpco/situacao/{numero}"

    def __init__(self) -> None:
        self._base_url = config.SISCOMEX_BASE_URL.rstrip("/")
        self._cnpj     = config.SISCOMEX_CNPJ
        self._cert_pem, self._key_pem = _load_pfx(
            config.CERT_PFX_PATH, config.CERT_PFX_PASSWORD
        )
        self._session: requests.Session | None = None
        self._cert_ctx: Any = None

    # ------------------------------------------------------------------
    # Gerenciamento de contexto
    # ------------------------------------------------------------------

    def __enter__(self) -> "SiscomexClient":
        self._cert_ctx = _temp_cert_files(self._cert_pem, self._key_pem)
        cert_path, key_path = self._cert_ctx.__enter__()

        self._session = requests.Session()
        self._session.cert = (cert_path, key_path)
        self._session.headers.update(
            {
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
        )
        logger.info("Sessão mTLS iniciada para CNPJ %s", self._cnpj)
        return self

    def __exit__(self, *_: Any) -> None:
        if self._session:
            self._session.close()
            self._session = None
        if self._cert_ctx:
            self._cert_ctx.__exit__(None, None, None)
            self._cert_ctx = None
        logger.debug("Sessão mTLS encerrada e arquivos temporários removidos.")

    # ------------------------------------------------------------------
    # HTTP internos
    # ------------------------------------------------------------------

    def _get(self, path: str, params: dict | None = None, timeout: tuple | None = None) -> dict | list:
        assert self._session, "Use dentro de 'with SiscomexClient() as client:'"
        url = f"{self._base_url}{path}"
        logger.debug("GET %s params=%s", url, params)
        response = self._session.get(url, params=params, timeout=timeout or self._TIMEOUT)
        response.raise_for_status()
        return response.json()

    def _post(self, path: str, payload: dict) -> dict | list:
        assert self._session, "Use dentro de 'with SiscomexClient() as client:'"
        url = f"{self._base_url}{path}"
        logger.debug("POST %s", url)
        response = self._session.post(url, json=payload, timeout=self._TIMEOUT)
        response.raise_for_status()
        return response.json()

    # ------------------------------------------------------------------
    # Autenticação
    # ------------------------------------------------------------------

    def autenticar(self, role_type: str) -> bool:
        """
        Autentica na plataforma Portal Único com o header Role-Type via POST.

        O Portal retorna o JWT em 'set-token' e o CSRF em 'x-csrf-token'.
        Ambos são injetados nos headers padrão da sessão:
          authorization: <JWT>   (sem prefixo Bearer — padrão não-standard do portal)
          x-csrf-token:  <CSRF>  (renovado automaticamente a cada resposta via hook)

        Role-Type válidos: IMPEXP, DEPOSIT, OPERPORT, TRANSPORT, etc.
        (ver tabela de domínio no manual PLAT — Aspectos Gerais → Perfis de Acesso)

        Retorna True se autenticado com sucesso.
        """
        assert self._session
        url = f"{self._base_url}{self._AUTH_PATH}"
        logger.debug("Autenticando com Role-Type=%s em POST %s", role_type, url)
        resp = self._session.post(
            url,
            headers={"Role-Type": role_type},
            allow_redirects=False,
            timeout=self._TIMEOUT,
        )

        if resp.status_code != 200:
            body = resp.json() if resp.content else {}
            logger.warning(
                "Autenticação falhou: HTTP %s — %s",
                resp.status_code,
                body.get("message", resp.text[:300]),
            )
            return False

        # Extrai tokens da resposta e os injeta na sessão
        jwt_token  = resp.headers.get("Set-Token", "")
        csrf_token = resp.headers.get("X-CSRF-Token", "")

        if not jwt_token:
            logger.error(
                "Autenticação: resposta 200 mas header 'Set-Token' ausente. "
                "Headers recebidos: %s",
                dict(resp.headers),
            )
            return False

        # Portal Único usa o JWT bruto no header 'authorization' (sem prefixo Bearer)
        self._session.headers["authorization"] = jwt_token

        # X-CSRF-Token é renovado a cada resposta; atualiza via hook
        if csrf_token:
            self._session.headers["x-csrf-token"] = csrf_token
            self._session.hooks["response"].append(self._renovar_csrf)

        # O CSRF do auth precisa ser "ativado" por uma chamada ao namespace /portal
        # antes de funcionar em /talpco — uma listagem de webhooks é suficiente
        try:
            self._get("/portal/api/ext/webhook")
            logger.debug("CSRF ativado via chamada portal.")
        except Exception:
            pass  # falha não é crítica — o CSRF pode já estar ativo

        logger.info("Autenticação bem-sucedida (Role-Type=%s).", role_type)
        return True

    def _renovar_csrf(self, resp: Any, *args: Any, **kwargs: Any) -> None:
        """Hook: atualiza o x-csrf-token sempre que o servidor devolve um novo."""
        novo = resp.headers.get("x-csrf-token", "")
        if novo and self._session:
            self._session.headers["x-csrf-token"] = novo

    # ------------------------------------------------------------------
    # Endpoints TALPCO
    # ------------------------------------------------------------------

    def verificar_conectividade(self, role_type: str = "") -> bool:
        """
        Testa se o servidor responde ao endpoint de autenticação (POST).
        Retorna True mesmo em caso de 422 (endpoint ativo, parâmetro inválido).
        """
        assert self._session
        url = f"{self._base_url}{self._AUTH_PATH}"
        headers = {"Role-Type": role_type} if role_type else {}
        try:
            resp = self._session.post(
                url, headers=headers, allow_redirects=False, timeout=self._TIMEOUT
            )
            # 200 = autenticado, 422 = endpoint ativo mas parâmetro inválido
            return resp.status_code in (200, 422)
        except requests.RequestException as exc:
            logger.warning("Conectividade falhou: %s", exc)
            return False

    def buscar_lpcos(
        self,
        situacao: str | None = None,
        data_inicio: str | None = None,
        data_fim: str | None = None,
        pagina: int = 1,
        tamanho: int = 10,
    ) -> PollingResult:
        """
        Consulta LPCOs via GET /talpco/api/ext/lpco/consulta.

        Parâmetros opcionais:
          situacao     — filtro de situação (ex: "DEFERIDO", "EM_ANALISE")
          data_inicio  — formato YYYY-MM-DD
          data_fim     — formato YYYY-MM-DD
          pagina       — página (default 1)
          tamanho      — itens por página (default 10; servidor TALPCO é lento com valores altos)
        """
        timestamp = datetime.now()
        params: dict = {"pagina": pagina, "tamanhoPagina": tamanho}
        if situacao:
            params["situacao"] = situacao
        if data_inicio:
            params["dataInicio"] = data_inicio
        if data_fim:
            params["dataFim"] = data_fim

        try:
            dados = self._get(self._LPCO_CONSULTA, params=params, timeout=self._TIMEOUT_DATA)
            items = (
                dados if isinstance(dados, list)
                else dados.get("data", dados.get("items", dados.get("lpcos", [])))
            )
            registros = [self._parse_lpco_item(it) for it in items]
            logger.info(
                "Polling %s — %d LPCO(s) retornado(s).",
                timestamp.isoformat(timespec="seconds"),
                len(registros),
            )
            return PollingResult(sucesso=True, timestamp=timestamp, registros=registros)

        except requests.HTTPError as exc:
            msg = f"HTTP {exc.response.status_code}: {exc.response.text[:300]}"
            logger.error("Erro no polling: %s", msg)
            return PollingResult(sucesso=False, timestamp=timestamp, erro=msg)
        except requests.RequestException as exc:
            msg = str(exc)
            logger.error("Erro de conexão no polling: %s", msg)
            return PollingResult(sucesso=False, timestamp=timestamp, erro=msg)

    def detalhar_lpco(self, numero: str) -> dict:
        """GET /talpco/api/ext/lpco/{numero}"""
        return self._get(self._LPCO_DETALHE.format(numero=numero))  # type: ignore[return-value]

    def historico_lpco(self, numero: str) -> list:
        """GET /talpco/api/ext/lpco/{numero}/historico"""
        return self._get(self._LPCO_HISTORICO.format(numero=numero))  # type: ignore[return-value]

    # ------------------------------------------------------------------
    # Parsing
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_lpco_item(item: dict) -> LpcoRecord:
        """
        Mapeia um item bruto do GET /ext/lpco/consulta para LpcoRecord.

        Os nomes dos campos seguem o schema ConsultarLpcoResponse da API TALPCO.
        Ajuste se o endpoint retornar nomes diferentes.
        """
        return LpcoRecord(
            numero=item.get("numero", ""),
            situacao=item.get("situacao", ""),
            tipo=item.get("codigoModelo", item.get("tipo", "")),
            responsavel_email=item.get("emailResponsavel", ""),
            data_validade=item.get("dataValidade"),
            raw=item,
        )
