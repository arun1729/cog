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

# ---------------------------------------------------------------------------
# Legacy constants (duplicated here so the migrate module is self-contained
# and doesn't require the removed LegacyCodec class).
# ---------------------------------------------------------------------------
_RECORD_SEP = b'\xFD'
_UNIT_SEP = b'\xAC'
_LEGACY_KEY_LINK_LEN = 16
_LEGACY_INDEX_BLOCK_LEN = 32
_SPINDLE_INDEX_BLOCK_LEN = 8


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

def _migrate_store(legacy_path):
    """Convert a single legacy store file to Spindle format.

    Returns the old_pos -> new_pos mapping (needed for index conversion),
    or None if the file is already Spindle or empty.
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
# Index migration
# ---------------------------------------------------------------------------

def _migrate_index(index_path, pos_map):
    """Convert a single legacy 32-byte-per-slot index to 8-byte-per-slot."""
    legacy_empty = '-1'.zfill(_LEGACY_INDEX_BLOCK_LEN).encode()
    spindle_empty = b'\x00' * _SPINDLE_INDEX_BLOCK_LEN

    file_size = os.path.getsize(index_path)
    if file_size % _LEGACY_INDEX_BLOCK_LEN != 0:
        raise ValueError(
            f"Index file {index_path} size {file_size} is not a multiple "
            f"of legacy block length {_LEGACY_INDEX_BLOCK_LEN}"
        )
    num_slots = file_size // _LEGACY_INDEX_BLOCK_LEN

    tmp_path = index_path + '.v4_tmp'
    with open(index_path, 'rb') as src, open(tmp_path, 'wb') as dst:
        for _ in range(num_slots):
            block = src.read(_LEGACY_INDEX_BLOCK_LEN)
            if block == legacy_empty:
                dst.write(spindle_empty)
            else:
                old_pos = int(block)
                new_pos = pos_map.get(old_pos)
                if new_pos is None:
                    dst.write(spindle_empty)
                else:
                    dst.write(struct.pack('<q', new_pos))


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
# Public API
# ---------------------------------------------------------------------------

STORE_MARKER = '-store-'
INDEX_MARKER = '-index-'


def migrate(db_path, remove_backups=False):
    """Migrate all legacy 3.x files under *db_path* to Spindle 4.x format.

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

        for key, store_path in store_files.items():
            try:
                pos_map = _migrate_store(store_path)
            except Exception as e:
                stats['errors'].append(f"{store_path}: {e}")
                _cleanup_temps(ns_dir)
                continue

            if pos_map is None:
                stats['skipped'] += 1
                continue

            idx_paths = index_files.get(key, [])
            idx_ok = True
            for idx_path in idx_paths:
                try:
                    _migrate_index(idx_path, pos_map)
                except Exception as e:
                    stats['errors'].append(f"{idx_path}: {e}")
                    idx_ok = False
                    break

            if not idx_ok:
                _cleanup_temps(ns_dir)
                continue

            # All files for this table converted — swap atomically
            _swap_files(store_path)
            stats['stores_migrated'] += 1
            for idx_path in idx_paths:
                _swap_files(idx_path)
                stats['indexes_migrated'] += 1

            if remove_backups:
                backup = store_path + '.v3_backup'
                if os.path.exists(backup):
                    os.remove(backup)
                for idx_path in idx_paths:
                    backup = idx_path + '.v3_backup'
                    if os.path.exists(backup):
                        os.remove(backup)

    return stats
