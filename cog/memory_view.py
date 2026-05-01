"""
In-memory adjacency graph for fast multi-hop traversals.

Maintains outgoing and incoming edge dicts that are kept in sync
with the on-disk store via add_edge / remove_edge calls from Graph.

Uses dicts-as-ordered-sets to preserve insertion order from disk,
which matters for tests that assert result ordering.

Pure Python, no external dependencies.
"""

from cog.database import hash_predicate

_PRESENT = True


class MemoryView:

    def __init__(self):
        self._out = {}  # src -> dict{target: True}  (ordered set)
        self._in = {}   # tgt -> dict{source: True}

    def load_from_table(self, table):
        out = {}
        inv = {}
        for record in table.indexer.scanner(table.store):
            key_bytes = record.key
            if not isinstance(key_bytes, (bytes, bytearray)):
                continue
            prefix = key_bytes[0:1]
            node = key_bytes[1:].decode('utf-8')
            targets = dict.fromkeys(record.value, _PRESENT)
            if prefix == b'\x00':
                out[node] = targets
            elif prefix == b'\x01':
                inv[node] = targets
        self._out = out
        self._in = inv

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

    def get_out(self, node_id):
        return self._out.get(node_id)

    def get_in(self, node_id):
        return self._in.get(node_id)
