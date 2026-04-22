"""Record codecs for CogDB on-disk format.

CogDB supports three on-disk formats:

- v0: legacy marshal-based, byte-16 flag = 0x30 (ASCII '0', pre-commit e847922).
- v1: legacy marshal-based, byte-16 flag = 0x31 (ASCII '1', post-commit e847922).
- Spindle (on-disk version 2): binary header + msgpack payload + per-record int64 ns timestamp.

v0 and v1 share LegacyCodec and differ only in the flag byte written into each
record's format-version slot. A file's format is decided once, on Store open,
and never changes for the lifetime of the file.

Spindle layout:

    File header (offset 0, 23 bytes):
        [magic 6]'COGDB\\x00'  [version 1]0x02  [created_at 8 int64 LE]  [reserved 8 zeros]

    Record (little-endian, no separators):
        [key_link 8 int64]  [value_type 1]  [timestamp 8 int64]
        [value_len varint 1..5]  [payload N bytes msgpack (key,value)]
        [value_link 8 int64]   -- only if value_type is list (0x01) or set (0x02)

Varint scheme (a minimal subset of msgpack positive-uint framing):
    tag <= 0x7f              -> value = tag                     (1 byte)
    tag == 0xcc              -> value = next uint8              (2 bytes)
    tag == 0xcd              -> value = next uint16 big-endian  (3 bytes)
    tag == 0xce              -> value = next uint32 big-endian  (5 bytes)
"""
import marshal
import struct
import time

import msgpack


V2_MAGIC = b'COGDB\x00'
V2_VERSION = 0x02
V2_HEADER_SIZE = 23

V2_VALUE_TYPE_STR = 0x00
V2_VALUE_TYPE_LIST = 0x01
V2_VALUE_TYPE_SET = 0x02

_V2_BYTE_TO_CHAR = {V2_VALUE_TYPE_STR: 's', V2_VALUE_TYPE_LIST: 'l', V2_VALUE_TYPE_SET: 'u'}
_V2_CHAR_TO_BYTE = {'s': V2_VALUE_TYPE_STR, 'l': V2_VALUE_TYPE_LIST, 'u': V2_VALUE_TYPE_SET}

RECORD_SEP = b'\xFD'
UNIT_SEP = b'\xAC'
RECORD_LINK_LEN = 16

LEGACY_V0_FLAG = 0x30  # ASCII '0', pre-e847922 tombstone default
LEGACY_V1_FLAG = 0x31  # ASCII '1', current CURRENT_FORMAT_VERSION


def _read_exactly(fh, n):
    """Read exactly n bytes from fh, or return None on EOF. Returns partial
    bytes if EOF is hit mid-read (caller must check length)."""
    if n == 0:
        return b''
    data = fh.read(n)
    if len(data) == 0:
        return None
    while len(data) < n:
        chunk = fh.read(n - len(data))
        if len(chunk) == 0:
            return data
        data += chunk
    return data


def _encode_varint(length):
    if length <= 0x7f:
        return bytes([length])
    if length <= 0xff:
        return b'\xcc' + bytes([length])
    if length <= 0xffff:
        return b'\xcd' + struct.pack('>H', length)
    if length <= 0xffffffff:
        return b'\xce' + struct.pack('>I', length)
    raise ValueError("value_len exceeds 2^32-1: " + str(length))


def _decode_varint(buf, offset):
    """Return (value, size_in_bytes)."""
    tag = buf[offset]
    if tag <= 0x7f:
        return tag, 1
    if tag == 0xcc:
        return buf[offset + 1], 2
    if tag == 0xcd:
        return struct.unpack_from('>H', buf, offset + 1)[0], 3
    if tag == 0xce:
        return struct.unpack_from('>I', buf, offset + 1)[0], 5
    raise ValueError("unknown varint tag: " + hex(tag))


class LegacyCodec:
    """v0 / v1 marshal-based format."""
    HEADER_SIZE = 0

    def __init__(self, version_flag=LEGACY_V1_FLAG):
        self.version_flag = version_flag
        self.VERSION = 0 if version_flag == LEGACY_V0_FLAG else 1
        # Character form used when encoding a record's format_version slot.
        self._flag_char = chr(version_flag)

    def write_header(self, fh):
        return

    def encode_record(self, record):
        # The codec's version_flag is authoritative — it matches the file's
        # existing format. The record's in-memory format_version is ignored on
        # write to guarantee no mixed-format files.
        key_link_bytes = str(record.key_link).encode().rjust(RECORD_LINK_LEN)
        serialized = marshal.dumps((record.key, record.value))
        m_record = (
            key_link_bytes
            + bytes([self.version_flag])
            + record.value_type.encode()
            + str(len(serialized)).encode()
            + UNIT_SEP
            + serialized
        )
        if record.value_type == 'l' or record.value_type == 'u':
            if record.value_link is not None:
                m_record += str(record.value_link).encode()
        m_record += RECORD_SEP
        return m_record

    def decode_record(self, store_bytes):
        from cog.core import Record
        key_link = int(store_bytes[0:RECORD_LINK_LEN])
        format_version = store_bytes[RECORD_LINK_LEN:RECORD_LINK_LEN + 1].decode()
        value_type = store_bytes[RECORD_LINK_LEN + 1:RECORD_LINK_LEN + 2].decode()
        value_len_buf, end_pos = self._read_until(RECORD_LINK_LEN + 2, store_bytes, UNIT_SEP)
        value_len = int(value_len_buf.decode())
        payload = store_bytes[end_pos + 1: end_pos + 1 + value_len]
        kv = marshal.loads(payload)

        value_link = Record.VALUE_LINK_NULL
        if value_type == 'l' or value_type == 'u':
            vl_buf, _ = self._read_until(end_pos + value_len + 1, store_bytes, RECORD_SEP)
            value_link = int(vl_buf.decode())

        return Record(kv[0], kv[1], format_version=format_version, store_position=None,
                      value_type=value_type, key_link=key_link, value_link=value_link)

    def read_record(self, fh):
        # Fixed 18-byte header: key_link(16) + flag(1) + value_type(1)
        header = _read_exactly(fh, 18)
        if header is None or len(header) < 18:
            return None

        value_type = chr(header[17])
        if value_type not in ('s', 'l', 'u'):
            return None

        # Variable-length value_len ASCII digits terminated by UNIT_SEP
        len_buf = b''
        while True:
            b = _read_exactly(fh, 1)
            if b is None:
                return None
            if b == UNIT_SEP:
                break
            len_buf += b
        try:
            value_len = int(len_buf.decode())
        except ValueError:
            return None

        payload = _read_exactly(fh, value_len)
        if payload is None or len(payload) < value_len:
            return None

        tail = b''
        if value_type in ('l', 'u'):
            while True:
                b = _read_exactly(fh, 1)
                if b is None:
                    break
                tail += b
                if b == RECORD_SEP:
                    break
        else:
            sep = _read_exactly(fh, 1)
            if sep is not None:
                tail = sep

        return header + len_buf + UNIT_SEP + payload + tail

    def update_key_link(self, fh, pos, new_link):
        fh.seek(pos)
        fh.write(self.key_link_bytes(new_link))

    def key_link_bytes(self, new_link):
        if type(new_link) is not int:
            raise ValueError("store position must be int but provided : " + str(new_link))
        return str(new_link).encode().rjust(RECORD_LINK_LEN)

    @staticmethod
    def _read_until(start, sbytes, separator):
        buff = b''
        i = start
        for i in range(start, len(sbytes)):
            s_byte = sbytes[i: i + 1]
            if s_byte == separator:
                break
            buff += s_byte
        return buff, i


class SpindleCodec:
    """Binary length-addressable format with file header and per-record timestamps."""
    VERSION = 2
    HEADER_SIZE = V2_HEADER_SIZE

    def __init__(self, created_at=None):
        # None means this codec was attached to a brand-new file that has not
        # had its header written yet. write_header() will stamp time.time_ns().
        self.created_at = created_at

    def write_header(self, fh):
        if self.created_at is None:
            self.created_at = time.time_ns()
        header = (
            V2_MAGIC
            + bytes([V2_VERSION])
            + struct.pack('<q', self.created_at)
            + b'\x00' * 8
        )
        assert len(header) == V2_HEADER_SIZE
        fh.seek(0)
        fh.write(header)

    def encode_record(self, record):
        key_link = record.key_link if record.key_link is not None else -1
        ts = record.timestamp if record.timestamp is not None else 0
        vtype = _V2_CHAR_TO_BYTE[record.value_type]
        payload = msgpack.packb((record.key, record.value), use_bin_type=True)
        out = (
            struct.pack('<q', key_link)
            + bytes([vtype])
            + struct.pack('<q', ts)
            + _encode_varint(len(payload))
            + payload
        )
        if record.value_type == 'l' or record.value_type == 'u':
            vl = record.value_link if record.value_link is not None else -1
            out += struct.pack('<q', vl)
        return out

    def decode_record(self, raw_bytes):
        from cog.core import Record
        key_link = struct.unpack_from('<q', raw_bytes, 0)[0]
        vtype_byte = raw_bytes[8]
        value_type = _V2_BYTE_TO_CHAR[vtype_byte]
        timestamp = struct.unpack_from('<q', raw_bytes, 9)[0]
        value_len, varint_size = _decode_varint(raw_bytes, 17)
        payload_start = 17 + varint_size
        payload = raw_bytes[payload_start: payload_start + value_len]
        key, value = msgpack.unpackb(payload, raw=False)

        value_link = Record.VALUE_LINK_NULL
        if value_type == 'l' or value_type == 'u':
            vl_offset = payload_start + value_len
            value_link = struct.unpack_from('<q', raw_bytes, vl_offset)[0]

        rec = Record(key, value, format_version='2', store_position=None,
                     value_type=value_type, key_link=key_link, value_link=value_link)
        rec.timestamp = timestamp
        return rec

    def read_record(self, fh):
        # Fixed 17 bytes: key_link(8) + value_type(1) + timestamp(8)
        header = _read_exactly(fh, 17)
        if header is None or len(header) < 17:
            return None
        vtype_byte = header[8]
        if vtype_byte not in _V2_BYTE_TO_CHAR:
            return None
        value_type = _V2_BYTE_TO_CHAR[vtype_byte]

        tag = _read_exactly(fh, 1)
        if tag is None:
            return None
        t = tag[0]
        if t <= 0x7f:
            value_len = t
            varint = tag
        elif t == 0xcc:
            rest = _read_exactly(fh, 1)
            if rest is None or len(rest) < 1:
                return None
            value_len = rest[0]
            varint = tag + rest
        elif t == 0xcd:
            rest = _read_exactly(fh, 2)
            if rest is None or len(rest) < 2:
                return None
            value_len = struct.unpack('>H', rest)[0]
            varint = tag + rest
        elif t == 0xce:
            rest = _read_exactly(fh, 4)
            if rest is None or len(rest) < 4:
                return None
            value_len = struct.unpack('>I', rest)[0]
            varint = tag + rest
        else:
            return None

        payload = _read_exactly(fh, value_len)
        if payload is None or len(payload) < value_len:
            return None

        tail = b''
        if value_type in ('l', 'u'):
            vl = _read_exactly(fh, 8)
            if vl is None or len(vl) < 8:
                return None
            tail = vl

        return header + varint + payload + tail

    def update_key_link(self, fh, pos, new_link):
        fh.seek(pos)
        fh.write(self.key_link_bytes(new_link))

    def key_link_bytes(self, new_link):
        if type(new_link) is not int:
            raise ValueError("store position must be int but provided : " + str(new_link))
        return struct.pack('<q', new_link)


def detect_codec(fh, file_size):
    """Decide which codec owns an open store file.

    Contract: fh is an rb+ handle. file_size is os.fstat size at open time.
    For empty files, returns a SpindleCodec with created_at=None; the caller must
    call write_header() to stamp and persist it. For existing files, the codec
    is fully initialised. The function seeks but does not write."""
    if file_size == 0:
        return SpindleCodec(created_at=None)

    fh.seek(0)
    prefix = fh.read(min(6, file_size))
    if prefix == V2_MAGIC:
        if file_size < V2_HEADER_SIZE:
            raise ValueError(
                "Spindle store file header truncated: got %d bytes, need %d"
                % (file_size, V2_HEADER_SIZE)
            )
        version_byte = fh.read(1)
        if version_byte[0] != V2_VERSION:
            raise ValueError("unsupported Spindle version: " + hex(version_byte[0]))
        created_at_bytes = fh.read(8)
        created_at = struct.unpack('<q', created_at_bytes)[0]
        return SpindleCodec(created_at=created_at)

    # A non-empty file shorter than the V2 magic that matches its prefix is a
    # torn V2 header write, not a legacy record. Reject it.
    if len(prefix) < 6 and V2_MAGIC.startswith(prefix):
        raise ValueError(
            "Spindle store file header truncated: got %d bytes, need %d"
            % (file_size, V2_HEADER_SIZE)
        )

    # Legacy: the flag byte lives at offset 16 of the first record. If a
    # legacy first-record write was torn below 17 bytes, fall back to the
    # most-recent legacy format so the Store can still be opened for recovery;
    # truncated reads will fail gracefully downstream.
    if file_size < 17:
        return LegacyCodec(version_flag=LEGACY_V1_FLAG)
    fh.seek(16)
    flag_byte = fh.read(1)[0]
    if flag_byte == LEGACY_V0_FLAG:
        return LegacyCodec(version_flag=LEGACY_V0_FLAG)
    if flag_byte == LEGACY_V1_FLAG:
        return LegacyCodec(version_flag=LEGACY_V1_FLAG)
    raise ValueError("unrecognized store format; byte 16 = " + hex(flag_byte))
