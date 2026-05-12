"""
In-memory adjacency view with demand paging.

Loads edges from a predicate table in pages using a resumable scanner.
On a cache miss, falls back to a single-key disk read and caches the
result so the same vertex never misses twice.

Writes are applied inline via add_edge / remove_edge so the view
never goes stale relative to disk.

Pure Python, no external dependencies.
"""

from cog.database import out_nodes, in_nodes

_PRESENT = True

DEFAULT_PAGE_SIZE = 50_000


class MemoryView:

    def __init__(self, table, page_size=None, shared_out=None, shared_in=None):
        self._table = table
        self._page_size = page_size or DEFAULT_PAGE_SIZE
        self._out = shared_out if shared_out is not None else {}
        self._in = shared_in if shared_in is not None else {}
        self._shared = shared_out is not None
        if self._shared:
            self._scanner = None
            self._fully_loaded = True
        else:
            self._scanner = table.indexer.scanner(table.store)
            self._fully_loaded = False
            self._load_page()

    def _load_page(self):
        if self._fully_loaded:
            return
        count = 0
        for record in self._scanner:
            self._ingest_record(record)
            count += 1
            if count >= self._page_size:
                return
        self._fully_loaded = True
        self._scanner = None

    def _ingest_record(self, record):
        key_bytes = record.key
        if not isinstance(key_bytes, (bytes, bytearray)):
            return
        prefix = key_bytes[0:1]
        node = key_bytes[1:].decode('utf-8')
        targets = dict.fromkeys(record.value, _PRESENT)
        if prefix == b'\x00':
            self._out[node] = targets
        elif prefix == b'\x01':
            self._in[node] = targets

    def _demand_load(self, node_id, direction):
        """Single-key disk read on cache miss. Caches the result."""
        indexer = self._table.indexer
        store = self._table.store
        if direction == 'out':
            record = indexer.get(out_nodes(node_id), store)
            if record is not None:
                self._out[node_id] = dict.fromkeys(record.value, _PRESENT)
                return self._out[node_id]
            self._out[node_id] = {}
            return None
        else:
            record = indexer.get(in_nodes(node_id), store)
            if record is not None:
                self._in[node_id] = dict.fromkeys(record.value, _PRESENT)
                return self._in[node_id]
            self._in[node_id] = {}
            return None

    def load_more(self):
        self._load_page()

    @property
    def fully_loaded(self):
        return self._fully_loaded

    def add_edge(self, src, tgt):
        o = self._out.get(src)
        if o is None:
            self._out[src] = {tgt: _PRESENT}
        else:
            o[tgt] = _PRESENT
        i = self._in.get(tgt)
        if i is None:
            self._in[tgt] = {src: _PRESENT}
        else:
            i[src] = _PRESENT

    def remove_edge(self, src, tgt):
        o = self._out.get(src)
        if o is not None:
            o.pop(tgt, None)
        i = self._in.get(tgt)
        if i is not None:
            i.pop(src, None)

    def replace_out(self, src, new_tgt):
        old = self._out.get(src)
        if old:
            for t in old:
                i = self._in.get(t)
                if i is not None:
                    i.pop(src, None)
        self._out[src] = {new_tgt: _PRESENT}
        i = self._in.get(new_tgt)
        if i is None:
            self._in[new_tgt] = {src: _PRESENT}
        else:
            i[src] = _PRESENT

    def clear(self):
        self._out.clear()
        self._in.clear()
        if not self._shared:
            self._scanner = self._table.indexer.scanner(self._table.store)
            self._fully_loaded = False

    def get_out(self, node_id):
        result = self._out.get(node_id)
        if result is not None:
            return result if result else None
        if not self._fully_loaded:
            return self._demand_load(node_id, 'out')
        return None

    def get_in(self, node_id):
        result = self._in.get(node_id)
        if result is not None:
            return result if result else None
        if not self._fully_loaded:
            return self._demand_load(node_id, 'in')
        return None
