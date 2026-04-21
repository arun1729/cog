"""
HTTP transport for CogDB Cloud.
"""

import json
import ssl
import urllib.request
import urllib.error

import certifi

from . import config as cfg

_SSL_CONTEXT = ssl.create_default_context(cafile=certifi.where())


class CloudClient:
    """Authenticated HTTP client for a single CogDB Cloud graph."""

    MAX_BATCH_SIZE = 500  # server-side limit per request

    def __init__(self, graph_name, api_key, flush_interval=1):
        self._graph_name = graph_name
        self._api_key = api_key
        self._base_url = f"{cfg.CLOUD_URL}{cfg.CLOUD_API_PREFIX}/{graph_name}"
        self._account_url = f"{cfg.CLOUD_URL}{cfg.CLOUD_API_PREFIX}/_cog_sys__"
        self._flush_interval = flush_interval
        self._pending = []  # buffered mutations awaiting flush

    def _request(self, method, path, body=None):
        """Make an authenticated request to a graph-scoped endpoint."""
        return self._do_request(method, f"{self._base_url}{path}", body)

    def _account_request(self, method, path, body=None):
        """Make an authenticated request to an account-scoped endpoint."""
        return self._do_request(method, f"{self._account_url}{path}", body)

    def _do_request(self, method, full_url, body=None):
        """Shared HTTP logic for all authenticated requests."""
        data = json.dumps(body).encode("utf-8") if body else None
        req = urllib.request.Request(full_url, data=data, method=method)
        req.add_header("Authorization", self._api_key)
        req.add_header("Content-Type", "application/json")
        req.add_header("User-Agent", "cogdb-python")

        try:
            with urllib.request.urlopen(req, context=_SSL_CONTEXT) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code in (401, 403):
                raise PermissionError("Invalid API key")
            try:
                detail = json.loads(e.read().decode("utf-8")).get("detail", "")
            except Exception:
                detail = ""
            if e.code in (400, 422):
                raise ValueError(detail or f"Bad request ({e.code})")
            raise RuntimeError(
                f"CogDB Cloud error ({e.code})" + (f": {detail}" if detail else "")
            )
        except urllib.error.URLError as e:
            raise ConnectionError(
                f"Cannot reach CogDB Cloud at {cfg.CLOUD_URL}: {e.reason}"
            )
        
    def _mutate_batch(self, mutations):
        """Send mutations via the batch endpoint, chunking at MAX_BATCH_SIZE."""
        total_count = 0
        for i in range(0, len(mutations), self.MAX_BATCH_SIZE):
            chunk = mutations[i:i + self.MAX_BATCH_SIZE]
            result = self._request("POST", "/mutate_batch", {
                "mutations": chunk,
            })
            total_count += result.get("count", len(chunk))
        return {"ok": True, "count": total_count}

    def _mutate_one(self, mutation):
        """Send a single mutation immediately (bypasses buffer)."""
        return self._mutate_batch([mutation])

    def _enqueue(self, mutation):
        """Buffer a mutation; auto-flush when flush_interval threshold is reached."""
        self._pending.append(mutation)
        if self._flush_interval > 0 and len(self._pending) >= self._flush_interval:
            self.sync()

    def sync(self):
        """Flush all pending mutations to cloud."""
        if not self._pending:
            return
        self._mutate_batch(list(self._pending))
        self._pending.clear()

    def mutate_put(self, subject, predicate, obj, update=False, create_new_edge=False):
        self._enqueue({
            "op": "PUT", "s": str(subject), "p": str(predicate), "o": str(obj),
            "update": update, "create_new_edge": create_new_edge,
        })

    def mutate_put_batch(self, triples):
        """triples: list of {"s": ..., "p": ..., "o": ...} dicts."""
        self.sync()  # flush pending before direct batch send
        mutations = [
            {"op": "PUT", "s": t["s"], "p": t["p"], "o": t["o"]}
            for t in triples
        ]
        return self._mutate_batch(mutations)

    def mutate_delete(self, subject, predicate, obj):
        self._enqueue({
            "op": "DELETE", "s": str(subject), "p": str(predicate), "o": str(obj),
        })

    def mutate_drop(self):
        self.sync()  # flush pending before destructive operation
        return self._mutate_one({"op": "DROP"})

    def mutate_truncate(self):
        self.sync()  # flush pending before destructive operation
        return self._mutate_one({"op": "TRUNCATE"})

    def mutate_put_embedding(self, word, embedding):
        return self._mutate_one({
            "op": "PUT_EMBEDDING", "word": word, "embedding": embedding,
        })

    def mutate_delete_embedding(self, word):
        return self._mutate_one({
            "op": "DELETE_EMBEDDING", "word": word,
        })

    def mutate_put_embeddings_batch(self, embeddings):
        """embeddings: list of {"word": ..., "embedding": ...} dicts."""
        mutations = [
            {"op": "PUT_EMBEDDING", "word": e["word"], "embedding": e["embedding"]}
            for e in embeddings
        ]
        return self._mutate_batch(mutations)

    def mutate_vectorize(self, words, provider, batch_size):
        return self._mutate_one({
            "op": "VECTORIZE", "words": words, "provider": provider,
            "batch_size": batch_size,
        })

    @staticmethod
    def _quote(value):
        """Quote a string value for the query string, escaping internal quotes and backslashes."""
        escaped = str(value).replace('\\', '\\\\').replace('"', '\\"')
        return f'"{escaped}"'

    @classmethod
    def _chain_to_query_string(cls, chain):
        """Convert a list of chain steps into a query string.

        Each step is a dict with 'method' and optional 'args'.
        Example chain:
            [{"method": "v", "args": {"vertex": "alice"}},
             {"method": "out", "args": {"predicates": ["knows"]}},
             {"method": "all"}]
        Result: v("alice").out("knows").all()
        """
        parts = []
        for step in chain:
            method = step["method"]
            args = step.get("args", {})
            param_str = cls._serialize_step(method, args)
            parts.append(f"{method}({param_str})")
        return ".".join(parts)

    @classmethod
    def _serialize_step(cls, method, args):
        """Serialize a step's args into its parameter string."""
        if not args:
            return ""

        if method == "v":
            vertex = args.get("vertex")
            if vertex is None:
                return ""
            if isinstance(vertex, list):
                items = ", ".join(cls._quote(v) for v in vertex)
                return f"[{items}]"
            return cls._quote(vertex)

        if method in ("out", "inc", "both"):
            predicates = args.get("predicates")
            if not predicates:
                return ""
            if len(predicates) == 1:
                return cls._quote(predicates[0])
            items = ", ".join(cls._quote(p) for p in predicates)
            return f"[{items}]"

        if method in ("has", "hasr"):
            predicates = args.get("predicates", [])
            vertex = args.get("vertex", "")
            if predicates and len(predicates) == 1:
                return f'{cls._quote(predicates[0])}, {cls._quote(vertex)}'
            if predicates:
                items = ", ".join(cls._quote(p) for p in predicates)
                return f'[{items}], {cls._quote(vertex)}'
            return cls._quote(vertex)

        if method == "is_":
            nodes = args.get("nodes", [])
            items = ", ".join(cls._quote(n) for n in nodes)
            return items

        if method == "tag":
            names = args.get("tag_names", [])
            return ", ".join(cls._quote(n) for n in names)

        if method == "back":
            return cls._quote(args.get("tag", ""))

        if method in ("limit", "skip"):
            return str(args.get("n", ""))

        if method == "order":
            return cls._quote(args.get("direction", "asc"))

        if method == "scan":
            parts = []
            if "limit" in args:
                parts.append(str(args["limit"]))
            if "scan_type" in args:
                parts.append(cls._quote(args["scan_type"]))
            return ", ".join(parts)

        if method == "all":
            options = args.get("options")
            if options:
                return cls._quote(options)
            return ""

        if method in ("bfs", "dfs"):
            parts = []
            predicates = args.get("predicates")
            if predicates:
                if len(predicates) == 1:
                    parts.append(cls._quote(predicates[0]))
                else:
                    items = ", ".join(cls._quote(p) for p in predicates)
                    parts.append(f"[{items}]")
            if args.get("max_depth") is not None:
                parts.append(str(args["max_depth"]))
            min_depth = args.get("min_depth")
            direction = args.get("direction")
            unique = args.get("unique")
            # Emit min_depth whenever a later positional arg is non-default
            has_later = ((direction is not None and direction != "out")
                         or (unique is not None and unique is not True))
            if (min_depth is not None and min_depth != 0) or has_later:
                parts.append(str(min_depth or 0))
            if direction is not None and direction != "out":
                parts.append(cls._quote(direction))
            if unique is not None and unique is not True:
                parts.append(str(unique).lower())
            return ", ".join(parts)

        if method == "sim":
            parts = [cls._quote(args.get("text", ""))]
            if args.get("operator"):
                parts.append(cls._quote(args["operator"]))
            if args.get("threshold") is not None:
                parts.append(str(args["threshold"]))
            if args.get("strict"):
                parts.append("true")
            return ", ".join(parts)

        if method == "k_nearest":
            parts = [cls._quote(args.get("text", ""))]
            if args.get("k") is not None:
                parts.append(str(args["k"]))
            return ", ".join(parts)

        # Fallback: serialize any remaining simple args
        return ", ".join(
            cls._quote(v) if isinstance(v, str) else str(v)
            for v in args.values()
        )

    def query_chain(self, chain):
        self.sync()  # flush pending for read-your-writes
        q = self._chain_to_query_string(chain)
        return self._request("POST", "/query", {"q": q})

    def query_scan(self, limit, scan_type):
        self.sync()
        q = f'scan({limit}, {self._quote(scan_type)})'
        return self._request("POST", "/query", {"q": q})

    def query_triples(self):
        self.sync()
        return self._request("POST", "/query", {"q": "triples()"})

    def query_get_embedding(self, word):
        self.sync()
        return self._request("POST", "/query", {
            "q": f'get_embedding({self._quote(word)})',
        })

    def query_scan_embeddings(self, limit):
        self.sync()
        return self._request("POST", "/query", {
            "q": f'scan_embeddings({limit})',
        })

    def query_embedding_stats(self):
        self.sync()
        return self._request("POST", "/query", {"q": "embedding_stats()"})


    def list_graphs(self):
        """List all graphs accessible by this API key.

        Returns:
            list[str]: Sorted list of graph names.
        """
        data = self._account_request("POST", "", {"fn": "ls"})
        graphs = data.get("graphs", data)
        return sorted(graphs) if isinstance(graphs, list) else graphs

