"""Migrate CogDB 3.x (legacy marshal-based) databases to 4.x (Spindle format).

Usage:
    from cog.migrate import migrate
    migrate("/path/to/cog-data")

The function walks the database directory, converts all legacy store files to
Spindle format and rebuilds the accompanying index files.  Original files are
renamed to *.v3_backup so the operation is reversible.

Files that are already in Spindle format are silently skipped.
"""

import marshal
import os
import struct
import time

from cog.codec import SpindleCodec, V2_MAGIC, V2_HEADER_SIZE
from cog import spindle_pack
from cog.config import INDEX_BLOCK_LEN as _NEW_INDEX_BLOCK_LEN, INDEX_CAPACITY as _NEW_INDEX_CAPACITY
from cog.config import GRAPH_NODE_SET_TABLE_NAME as _GRAPH_NODE_SET_TABLE_NAME
from cog.core import cog_hash

# ---------------------------------------------------------------------------
# Legacy constants (duplicated here so the migrate module is self-contained
# and doesn't require the removed LegacyCodec class).
# ---------------------------------------------------------------------------
_RECORD_SEP = b'\xFD'
_UNIT_SEP = b'\xAC'
_LEGACY_KEY_LINK_LEN = 16
_LEGACY_INDEX_BLOCK_LEN = 32

# Legacy torque encoded directional graph edge keys as string suffixes on the
# vertex id ("alice__:out:__"). v4 torque (cog.database.out_nodes/in_nodes)
# uses a 1-byte direction prefix on utf-8 bytes (b'\x00alice'). These keys hash
# to different slots and are different key values, so a faithful byte-for-byte
# migration would leave graph edges unreachable. Rewrite them here so migrated
# graphs are traversable.
#
# The rewrite is applied ONLY in graph namespaces (namespaces containing a
# TOR_NODE_SET table). In a plain KV namespace a user key that happens to end
# with "__:out:__" is legitimate data — rewriting it would silently rename the
# key and lose it.
_LEGACY_OUT_SUFFIX = "__:out:__"
_LEGACY_IN_SUFFIX = "__:in:__"
_OUT_PREFIX = b'\x00'
_IN_PREFIX = b'\x01'


def _migrate_edge_key(key):
    """Rewrite a legacy torque edge key to the v4 byte-prefix encoding.

    Returns the key unchanged if it is not a directional edge key.
    """
    if type(key) is str:
        if key.endswith(_LEGACY_OUT_SUFFIX):
            return _OUT_PREFIX + key[:-len(_LEGACY_OUT_SUFFIX)].encode('utf-8')
        if key.endswith(_LEGACY_IN_SUFFIX):
            return _IN_PREFIX + key[:-len(_LEGACY_IN_SUFFIX)].encode('utf-8')
    return key


def _read_exactly(fh, n):
    data = fh.read(n)
    if len(data) == 0:
        return None
    while len(data) < n:
        chunk = fh.read(n - len(data))
        if len(chunk) == 0:
            return data
        data += chunk
    return data


# ---------------------------------------------------------------------------
# Legacy record reader
# ---------------------------------------------------------------------------

def _read_legacy_record(fh):
    """Read one legacy record from *fh*, returning (position, fields) or None
    at EOF.  *position* is the file offset where the record started.

    Returns: (pos, key, value, value_type, key_link, value_link) or None.
    """
    pos = fh.tell()

    header = _read_exactly(fh, 18)
    if header is None or len(header) < 18:
        return None

    key_link = int(header[0:_LEGACY_KEY_LINK_LEN])
    # header[16] = format_version byte ('0' or '1'), ignored
    value_type = chr(header[17])
    if value_type not in ('s', 'l', 'u'):
        return None

    len_buf = b''
    while True:
        b = _read_exactly(fh, 1)
        if b is None:
            return None
        if b == _UNIT_SEP:
            break
        len_buf += b
    try:
        value_len = int(len_buf.decode())
    except ValueError:
        return None

    payload = _read_exactly(fh, value_len)
    if payload is None or len(payload) < value_len:
        return None

    kv = marshal.loads(payload)
    key, value = kv[0], kv[1]

    value_link = -1
    if value_type in ('l', 'u'):
        vl_buf = b''
        while True:
            b = _read_exactly(fh, 1)
            if b is None:
                break
            if b == _RECORD_SEP:
                break
            vl_buf += b
        if vl_buf:
            value_link = int(vl_buf.decode())
    else:
        _read_exactly(fh, 1)  # consume trailing RECORD_SEP

    return (pos, key, value, value_type, key_link, value_link)


# ---------------------------------------------------------------------------
# Store migration
# ---------------------------------------------------------------------------

def _migrate_store(legacy_path, rewrite_edge_keys=False):
    """Convert a single legacy store file to Spindle format.

    Returns the old_pos -> new_pos mapping (needed for index conversion),
    or None if the file is already Spindle or empty.

    *rewrite_edge_keys* must be True only for tables in graph namespaces (see
    _migrate_edge_key): there the legacy "__:out:__"/"__:in:__" suffix keys
    are torque edge keys and must be rewritten; anywhere else such keys are
    user data and must be preserved byte-for-byte.
    """
    with open(legacy_path, 'rb') as fh:
        head = fh.read(6)
        if len(head) == 0:
            return None
        if head == V2_MAGIC:
            return None  # already Spindle

    codec = SpindleCodec(created_at=time.time_ns())
    pos_map = {}  # old_position -> new_position

    tmp_path = legacy_path + '.v4_tmp'
    with open(legacy_path, 'rb') as src, open(tmp_path, 'wb') as dst:
        codec.write_header(dst)

        while True:
            result = _read_legacy_record(src)
            if result is None:
                break
            old_pos, key, value, value_type, old_key_link, old_value_link = result

            # Rewrite legacy graph edge keys to the v4 byte-prefix encoding so
            # migrated graphs remain traversable (see _migrate_edge_key).
            if rewrite_edge_keys:
                key = _migrate_edge_key(key)

            new_key_link = pos_map.get(old_key_link, -1) if old_key_link != -1 else -1
            new_value_link = pos_map.get(old_value_link, -1) if old_value_link != -1 else -1

            new_pos = dst.tell()
            pos_map[old_pos] = new_pos

            payload = spindle_pack.packb(key, value)
            from cog.codec import _encode_varint, _V2_CHAR_TO_BYTE
            vtype_byte = _V2_CHAR_TO_BYTE[value_type]
            varint = _encode_varint(len(payload))

            has_vlink = value_type in ('l', 'u')
            total = 17 + len(varint) + len(payload) + (8 if has_vlink else 0)
            out = bytearray(total)

            struct.pack_into('<q', out, 0, new_key_link)
            out[8] = vtype_byte
            struct.pack_into('<q', out, 9, 0)  # timestamp = 0 for migrated records
            p = 17
            out[p:p + len(varint)] = varint
            p += len(varint)
            out[p:p + len(payload)] = payload
            p += len(payload)
            if has_vlink:
                struct.pack_into('<q', out, p, new_value_link)

            dst.write(bytes(out))

    return pos_map


# ---------------------------------------------------------------------------
# Legacy liveness
# ---------------------------------------------------------------------------
#
# The legacy store is an append-only log: same-key updates append a new record
# and Cog.delete only unlinks the index entry, leaving the record in the file.
# Which records are *live* is therefore recorded exclusively in the legacy
# index (chain heads) and the key_link chains threaded through the store.
# Rebuilding the new index from the store log alone would resurrect deleted
# keys and chain every historical record (breaking the 4.x one-entry-per-key
# invariant that scanner/MemoryView rely on), so liveness must be derived by
# walking the legacy index.

_LEGACY_EMPTY_INDEX_BLOCK = '-1'.zfill(_LEGACY_INDEX_BLOCK_LEN).encode()


def _legacy_index_heads(index_path):
    """Yield the store positions stored as chain heads in a legacy index."""
    with open(index_path, 'rb') as fh:
        while True:
            block = fh.read(_LEGACY_INDEX_BLOCK_LEN)
            if len(block) < _LEGACY_INDEX_BLOCK_LEN:
                break
            if block == _LEGACY_EMPTY_INDEX_BLOCK:
                continue
            try:
                pos = int(block)
            except ValueError:
                continue  # unrecognized block, nothing to chase
            if pos >= 0:
                yield pos


def _collect_live_positions(legacy_index_paths, legacy_store_path):
    """Return the set of live record positions in the legacy store.

    A record is live iff it is reachable from a legacy index chain head and is
    the first record for its key along the chain (head -> tail order), which
    is exactly what the legacy Index.get returned. Records that were deleted
    (unlinked) or superseded by a newer same-key write are excluded.

    *legacy_index_paths* must be ordered the way the legacy Indexer searched
    them (ascending index id) so that a key present in several index files
    resolves to the same record the old version would have returned.
    """
    live = set()
    seen_keys = set()
    with open(legacy_store_path, 'rb') as sf:
        for idx_path in legacy_index_paths:
            for head in _legacy_index_heads(idx_path):
                pos = head
                visited = set()
                while pos != -1 and pos not in visited:
                    visited.add(pos)
                    sf.seek(pos)
                    result = _read_legacy_record(sf)
                    if result is None:
                        break  # dangling pointer / truncated record
                    _, key, _value, _vtype, key_link, _vlink = result
                    if key not in seen_keys:
                        seen_keys.add(key)
                        live.add(pos)
                    pos = key_link
    return live


# ---------------------------------------------------------------------------
# Index migration
# ---------------------------------------------------------------------------

def _migrate_index(index_path, migrated_store_path, live_new_positions):
    """Rebuild an index file containing exactly *live_new_positions*.

    Rather than translating legacy slot positions (which are tied to the old
    slot formula), we chain each live record of the migrated store into its
    slot under the current slot formula. Only live records are chained: the
    4.x Index.put invariant is that a key appears at most once in a bucket
    chain (scanner and MemoryView depend on it), and records deleted in 3.x
    must not reappear.

    Capacity is inferred from the legacy index file size so any user-custom
    capacity is preserved across the migration.
    """
    block_len = _NEW_INDEX_BLOCK_LEN
    legacy_size = os.path.getsize(index_path)
    if legacy_size % _LEGACY_INDEX_BLOCK_LEN != 0:
        raise ValueError(
            f"Index file {index_path} size {legacy_size} is not a multiple "
            f"of legacy block length {_LEGACY_INDEX_BLOCK_LEN}"
        )
    capacity = legacy_size // _LEGACY_INDEX_BLOCK_LEN

    slots = bytearray(capacity * block_len)

    codec = SpindleCodec(created_at=None)
    with open(migrated_store_path, 'rb+') as sf:
        if sf.read(6) != V2_MAGIC:
            raise ValueError(f"expected Spindle magic in {migrated_store_path}")

        for pos in sorted(live_new_positions):
            sf.seek(pos)
            raw = codec.read_record(sf)
            if raw is None:
                raise ValueError(
                    f"live record at position {pos} is unreadable in {migrated_store_path}")
            rec = codec.decode_record(raw)

            slot = cog_hash(rec.key, capacity)
            offset = slot * block_len
            existing_head = struct.unpack_from('<q', slots, offset)[0]

            # The record joins the head of its slot chain; its key_link points
            # to the previous head (a hash collision — readers walk key_link
            # until they find a matching key).
            new_key_link = existing_head if existing_head != 0 else -1
            sf.seek(pos)
            sf.write(struct.pack('<q', new_key_link))
            struct.pack_into('<q', slots, offset, pos)

    tmp_path = index_path + '.v4_tmp'
    with open(tmp_path, 'wb') as dst:
        dst.write(bytes(slots))


# ---------------------------------------------------------------------------
# Atomic swap helpers
# ---------------------------------------------------------------------------

def _swap_files(original, tmp_suffix='.v4_tmp', backup_suffix='.v3_backup'):
    """Rename original -> backup, tmp -> original."""
    tmp = original + tmp_suffix
    backup = original + backup_suffix
    if not os.path.exists(tmp):
        return
    if os.path.exists(backup):
        os.remove(backup)
    os.rename(original, backup)
    os.rename(tmp, original)


def _cleanup_temps(directory):
    for f in os.listdir(directory):
        if f.endswith('.v4_tmp'):
            os.remove(os.path.join(directory, f))


# ---------------------------------------------------------------------------
# Pre-flight
# ---------------------------------------------------------------------------

def _preflight_store(legacy_path, rewrite_edge_keys=False, max_errors=20):
    """Check that every record in a legacy store encodes in the new format.

    Returns a list of error strings (empty = fully migratable). Files already
    in Spindle format and empty files report no errors (migration skips them).
    """
    errors = []
    file_size = os.path.getsize(legacy_path)
    with open(legacy_path, 'rb') as fh:
        head = fh.read(6)
        if len(head) == 0 or head == V2_MAGIC:
            return errors
        fh.seek(0)
        while True:
            scan_pos = fh.tell()
            result = _read_legacy_record(fh)
            if result is None:
                # The sequential migration reader stops here too. If this is
                # not the true end of the file, everything after this point
                # would be silently dropped — treat it as unmigratable.
                if fh.tell() != file_size:
                    errors.append(
                        f"unparseable record at position {scan_pos}; "
                        f"{file_size - scan_pos} trailing bytes would be lost")
                break
            pos, key, value, _vtype, _klink, _vlink = result
            try:
                spindle_pack.packb(
                    _migrate_edge_key(key) if rewrite_edge_keys else key, value)
            except Exception as e:
                if len(errors) >= max_errors:
                    errors.append("... more unencodable records omitted")
                    break
                errors.append(f"record at {pos} (key={key!r}): {e}")
    return errors


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

STORE_MARKER = '-store-'
INDEX_MARKER = '-index-'


def migrate(db_path, remove_backups=False):
    """Migrate all legacy 3.x files under *db_path* to Spindle 4.x format.

    A pre-flight pass first verifies that every record in every legacy store
    can be represented in the new format. If any record cannot, ALL problems
    are reported in ``errors`` and nothing is converted, so the database is
    never left half-migrated.

    Args:
        db_path: Root database directory (the value of COG_HOME or
                 CUSTOM_COG_DB_PATH).
        remove_backups: If True, delete the .v3_backup files after a
                        successful migration instead of keeping them.

    Returns:
        dict with keys:
            stores_migrated (int): number of store files converted
            indexes_migrated (int): number of index files converted
            skipped (int): files already in Spindle format
            errors (list[str]): per-file error messages, if any

    Raises nothing — errors are collected and returned.
    """
    if not os.path.isdir(db_path):
        raise FileNotFoundError(f"Database path does not exist: {db_path}")

    stats = {'stores_migrated': 0, 'indexes_migrated': 0, 'skipped': 0, 'errors': []}

    # ---------------------------------------------------------- discovery
    namespaces = []  # (ns_dir, store_files, index_files, is_graph_ns)
    for ns_entry in sorted(os.listdir(db_path)):
        ns_dir = os.path.join(db_path, ns_entry)
        if not os.path.isdir(ns_dir):
            continue
        # Skip system directories
        if ns_entry in ('sys', 'views'):
            continue

        store_files = {}  # (table_name, instance_id) -> store_path
        index_files = {}  # (table_name, instance_id) -> [index_path, ...]

        for fname in os.listdir(ns_dir):
            fpath = os.path.join(ns_dir, fname)
            if not os.path.isfile(fpath):
                continue
            if fname.endswith(('.v3_backup', '.v4_tmp')):
                continue

            if STORE_MARKER in fname:
                parts = fname.split(STORE_MARKER)
                table_name = parts[0]
                instance_id = parts[1]
                store_files[(table_name, instance_id)] = fpath
            elif INDEX_MARKER in fname:
                parts = fname.split(INDEX_MARKER)
                table_name = parts[0]
                rest = parts[1]  # instance_id-index_id
                instance_id = rest.rsplit('-', 1)[0]
                key = (table_name, instance_id)
                index_files.setdefault(key, []).append(fpath)

        # Torque edge-key rewriting applies only to graph namespaces,
        # recognizable by their node-set table (see _migrate_edge_key).
        table_names = {t for t, _ in store_files} | {t for t, _ in index_files}
        is_graph_ns = _GRAPH_NODE_SET_TABLE_NAME in table_names

        namespaces.append((ns_dir, store_files, index_files, is_graph_ns))

    # ---------------------------------------------------------- pre-flight
    for ns_dir, store_files, _index_files, is_graph_ns in namespaces:
        for store_path in store_files.values():
            try:
                problems = _preflight_store(store_path, rewrite_edge_keys=is_graph_ns)
            except Exception as e:
                problems = [f"pre-flight scan failed: {e}"]
            stats['errors'].extend(f"{store_path}: {p}" for p in problems)
    if stats['errors']:
        # Report every problem up front and convert nothing.
        return stats

    # ---------------------------------------------------------- conversion
    for ns_dir, store_files, index_files, is_graph_ns in namespaces:
        for key, store_path in store_files.items():
            try:
                pos_map = _migrate_store(store_path, rewrite_edge_keys=is_graph_ns)
            except Exception as e:
                stats['errors'].append(f"{store_path}: {e}")
                _cleanup_temps(ns_dir)
                continue

            if pos_map is None:
                stats['skipped'] += 1
                continue

            migrated_store_path = store_path + '.v4_tmp'

            # Order index files by index id.  Only index-0 gets a rebuilt
            # replacement — the rebuild inserts every live key, so a single
            # index-0 file is sufficient.  Extra legacy index files (from
            # the old multi-index Indexer) still participate in the
            # liveness walk, then are renamed to .v3_backup without
            # producing replacements.
            idx_ok = True
            try:
                indexed = sorted(
                    (int(os.path.basename(p).rsplit('-', 1)[1]), p)
                    for p in index_files.get(key, [])
                )
            except ValueError as e:
                stats['errors'].append(f"{store_path}: cannot parse index file name: {e}")
                _cleanup_temps(ns_dir)
                continue
            primary_idx = next((p for i, p in indexed if i == 0), None)
            extra_idxs = [p for i, p in indexed if i != 0]

            if primary_idx is not None:
                try:
                    # Deletions and same-key updates in 3.x are recorded only
                    # in the legacy index chains — derive liveness from them,
                    # not from the append-only store log.
                    live_legacy = _collect_live_positions(
                        [p for _i, p in indexed], store_path)
                    live_new = {pos_map[p] for p in live_legacy if p in pos_map}
                    _migrate_index(primary_idx, migrated_store_path, live_new)
                except Exception as e:
                    stats['errors'].append(f"{primary_idx}: {e}")
                    idx_ok = False

            if not idx_ok:
                _cleanup_temps(ns_dir)
                continue

            # All files for this table converted — swap atomically
            _swap_files(store_path)
            stats['stores_migrated'] += 1
            if primary_idx is not None:
                _swap_files(primary_idx)
                stats['indexes_migrated'] += 1
            # Rename extra legacy index files to .v3_backup (no replacement)
            for idx_path in extra_idxs:
                backup = idx_path + '.v3_backup'
                if os.path.exists(backup):
                    os.remove(backup)
                os.rename(idx_path, backup)

            if remove_backups:
                backup = store_path + '.v3_backup'
                if os.path.exists(backup):
                    os.remove(backup)
                if primary_idx is not None:
                    backup = primary_idx + '.v3_backup'
                    if os.path.exists(backup):
                        os.remove(backup)
                for idx_path in extra_idxs:
                    backup = idx_path + '.v3_backup'
                    if os.path.exists(backup):
                        os.remove(backup)

    return stats
