"""Record codecs for CogDB on-disk format.

Spindle (on-disk version 2): binary header + spindle_pack payload + per-record
int64 ns timestamp.

Spindle layout:

    File header (offset 0, 23 bytes):
        [magic 6]'COGDB\\x00'  [version 1]0x02  [created_at 8 int64 LE]  [reserved 8 zeros]

    Record (little-endian, no separators):
        [key_link 8 int64]  [value_type 1]  [timestamp 8 int64]
        [value_len varint 1..5]  [payload N bytes spindle_pack (key,value)]
        [value_link 8 int64]   -- only if value_type is list (0x01) or set (0x02)

The payload uses cog.spindle_pack (see that module for wire format). Payloads
are length-addressable via the outer value_len varint; spindle_pack does not
carry its own length field for the fast-path value.

Varint scheme (little-endian):
    tag <= 0x7f              -> value = tag                        (1 byte)
    tag == 0xcc              -> value = next uint8                 (2 bytes)
    tag == 0xcd              -> value = next uint16 little-endian  (3 bytes)
    tag == 0xce              -> value = next uint32 little-endian  (5 bytes)
"""
import struct
import time

from cog.spindle_pack import _encode_varint, _decode_varint, packb, unpackb


V2_MAGIC = b'COGDB\x00'
V2_VERSION = 0x02
V2_HEADER_SIZE = 23

V2_VALUE_TYPE_STR = 0x00
V2_VALUE_TYPE_LIST = 0x01
V2_VALUE_TYPE_SET = 0x02

_V2_BYTE_TO_CHAR = {V2_VALUE_TYPE_STR: 's', V2_VALUE_TYPE_LIST: 'l', V2_VALUE_TYPE_SET: 'u'}
_V2_CHAR_TO_BYTE = {'s': V2_VALUE_TYPE_STR, 'l': V2_VALUE_TYPE_LIST, 'u': V2_VALUE_TYPE_SET}


# Varint tag -> number of extra bytes after the tag.
_VARINT_EXTRA = {0xcc: 1, 0xcd: 2, 0xce: 4}


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
        payload = packb(record.key, record.value)
        varint = _encode_varint(len(payload))

        has_vlink = record.value_type in ('l', 'u')
        total = 17 + len(varint) + len(payload) + (8 if has_vlink else 0)
        out = bytearray(total)

        struct.pack_into('<q', out, 0, key_link)
        out[8] = vtype
        struct.pack_into('<q', out, 9, ts)
        pos = 17
        out[pos:pos + len(varint)] = varint
        pos += len(varint)
        out[pos:pos + len(payload)] = payload
        pos += len(payload)

        if has_vlink:
            vl = record.value_link if record.value_link is not None else -1
            struct.pack_into('<q', out, pos, vl)

        return bytes(out)

    def decode_record(self, raw_bytes):
        rec, _ = self.decode_at(raw_bytes, 0)
        return rec

    def decode_at(self, buf, offset):
        from cog.core import Record
        key_link = struct.unpack_from('<q', buf, offset)[0]
        vtype_byte = buf[offset + 8]
        value_type = _V2_BYTE_TO_CHAR[vtype_byte]
        timestamp = struct.unpack_from('<q', buf, offset + 9)[0]
        value_len, varint_size = _decode_varint(buf, offset + 17)
        payload_start = offset + 17 + varint_size
        payload_end = payload_start + value_len
        key, value = unpackb(buf[payload_start: payload_end])

        value_link = Record.VALUE_LINK_NULL
        end = payload_end
        if value_type == 'l' or value_type == 'u':
            value_link = struct.unpack_from('<q', buf, payload_end)[0]
            end = payload_end + 8

        rec = Record(key, value, store_position=None,
                     value_type=value_type, key_link=key_link, value_link=value_link)
        rec.timestamp = timestamp
        return rec, end

    def read_record(self, fh):
        # Fixed 17 bytes: key_link(8) + value_type(1) + timestamp(8)
        header = _read_exactly(fh, 17)
        if header is None or len(header) < 17:
            return None
        vtype_byte = header[8]
        if vtype_byte not in _V2_BYTE_TO_CHAR:
            return None
        value_type = _V2_BYTE_TO_CHAR[vtype_byte]

        # Read varint: peek at tag to determine total size, then delegate
        # to _decode_varint for the actual value.
        tag = _read_exactly(fh, 1)
        if tag is None:
            return None
        t = tag[0]
        extra = _VARINT_EXTRA.get(t, 0) if t > 0x7f else 0
        if t > 0x7f and t not in _VARINT_EXTRA:
            return None

        if extra > 0:
            rest = _read_exactly(fh, extra)
            if rest is None or len(rest) < extra:
                return None
            varint_buf = tag + rest
        else:
            varint_buf = tag
        value_len, _ = _decode_varint(varint_buf, 0)

        payload = _read_exactly(fh, value_len)
        if payload is None or len(payload) < value_len:
            return None

        tail = b''
        if value_type in ('l', 'u'):
            vl = _read_exactly(fh, 8)
            if vl is None or len(vl) < 8:
                return None
            tail = vl

        return header + varint_buf + payload + tail

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

    raise ValueError(
        "unrecognized store format (legacy v0/v1 is no longer supported); "
        "first 6 bytes: " + repr(prefix)
    )
