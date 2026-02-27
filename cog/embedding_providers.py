"""Embedding provider functions for g.vectorize()."""
import json
import ssl
import urllib.request
import urllib.error
import logging
import certifi
from . import config as cfg

logger = logging.getLogger(__name__)

# Default timeout for HTTP requests (seconds)
_REQUEST_TIMEOUT = 30

# SSL context using certifi's CA bundle
_SSL_CONTEXT = ssl.create_default_context(cafile=certifi.where())


def _chunked(lst, size):
    """Yield successive chunks of `size` from `lst`."""
    for i in range(0, len(lst), size):
        yield lst[i:i + size]


def _provider_cogdb(texts, url=None, **kwargs):
    """
    CogDB embedding provider (default).
    POST {"texts": [...]} → {"embeddings": [{"text": ..., "vector": [...]}, ...]}
    Model: bge-small-en-v1.5 (384 dims). Free, no API key.
    """
    if url is None:
        url = cfg.COGDB_EMBED_URL
    results = []
    for chunk in _chunked(texts, 100):
        body = json.dumps({"texts": chunk}).encode("utf-8")
        req = urllib.request.Request(
            url, data=body,
            headers={"Content-Type": "application/json", "User-Agent": "CogDB"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=_REQUEST_TIMEOUT, context=_SSL_CONTEXT) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        for item in data["embeddings"]:
            results.append((item["text"], item["vector"]))
    return results


def _provider_openai(texts, api_key=None, model="text-embedding-3-small", **kwargs):
    """
    OpenAI embedding provider.
    POST {"model": ..., "input": [...]} → {"data": [{"index": N, "embedding": [...]}, ...]}
    Model: text-embedding-3-small (1536 dims). Requires api_key.
    """
    if not api_key:
        raise ValueError("api_key is required for the OpenAI provider")
    url = "https://api.openai.com/v1/embeddings"
    results = []
    for chunk in _chunked(texts, 100):
        body = json.dumps({"model": model, "input": chunk}).encode("utf-8")
        req = urllib.request.Request(
            url, data=body,
            headers={
                "Content-Type": "application/json",
                "Authorization": "Bearer {}".format(api_key),
                "User-Agent": "CogDB",
            },
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=_REQUEST_TIMEOUT, context=_SSL_CONTEXT) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        for item in data["data"]:
            results.append((chunk[item["index"]], item["embedding"]))
    return results


def _provider_custom(texts, url=None, **kwargs):
    """
    Custom provider — same request/response format as the CogDB provider,
    but accepts an arbitrary URL. Intended for trusted, developer-supplied
    endpoints only (not for untrusted user input).
    """
    if not url:
        raise ValueError("url is required for the custom provider")
    return _provider_cogdb(texts, url=url, **kwargs)


EMBEDDING_PROVIDERS = {
    "cogdb": _provider_cogdb,
    "openai": _provider_openai,
    "custom": _provider_custom,
}
