"""HTTP provider that keeps the error response body.

web3's shared aiohttp session uses raise_for_status, which drops the body
before the watcher can log it. This provider reads the body first and raises
RpcHttpError with the status and that text. The URL is not part of the error.
"""

from typing import Optional

from aiohttp import ClientSession
from web3.providers import AsyncHTTPProvider
from web3.types import RPCEndpoint, RPCResponse


class RpcHttpError(Exception):
    def __init__(self, status: int, reason: str, body: str):
        self.status = status
        self.reason = reason
        self.message = reason
        self.body = body
        super().__init__(f"HTTP {status} {reason}: {body}")


class ReadingHTTPProvider(AsyncHTTPProvider):
    def __init__(self, endpoint_uri=None, request_kwargs=None):
        super().__init__(endpoint_uri, request_kwargs)
        self._session: Optional[ClientSession] = None

    async def aclose(self) -> None:
        session = self._session
        self._session = None
        if session is not None and not session.closed:
            await session.close()

    async def _get_session(self) -> ClientSession:
        if self._session is None or self._session.closed:
            timeout = None
            if self._request_kwargs:
                timeout = self._request_kwargs.get("timeout")
            self._session = ClientSession(timeout=timeout) if timeout else ClientSession()
        return self._session

    async def make_request(self, method: RPCEndpoint, params) -> RPCResponse:
        session = await self._get_session()
        request_data = self.encode_rpc_request(method, params)
        kwargs = dict(self.get_request_kwargs())
        async with session.post(self.endpoint_uri, data=request_data, **kwargs) as response:
            raw = await response.read()
            if response.status >= 400:
                body = raw.decode("utf-8", "replace").replace("\n", " ").strip()
                if len(body) > 500:
                    body = body[:500]
                raise RpcHttpError(response.status, response.reason or "", body)
        return self.decode_rpc_response(raw)
