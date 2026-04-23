"""
Binary packer for key-value pairs.

Wire format per field:
    [type 1B]
    type 's' (0x73): [varint length] [utf-8 bytes]
    type 'i' (0x69): [8B int64 LE]               — struct.pack('<q')
    type 'f' (0x66): [8B float64 LE]              — struct.pack('<d')
    type 'b' (0x62): [varint length] [raw bytes]

Varint scheme (msgpack-compatible positive-uint subset, big-endian by convention):
    tag <= 0x7f         -> value = tag                    (1 byte total)
    tag == 0xcc         -> value = next uint8             (2 bytes total)
    tag == 0xcd         -> value = next uint16 big-endian (3 bytes total)
    tag == 0xce         -> value = next uint32 big-endian (5 bytes total)

Endianness note: varints use big-endian to match the msgpack convention they
are modeled after.  Fixed-width numeric fields (int64, float64) use little-
endian to match the SpindleCodec record layout in codec.py.

Bool is intentionally unsupported — Python's bool is an int subclass, but
CogDB's domain is strings and numbers only.
"""
import struct

# Pre-compiled struct formatters for hot-path numeric fields.
_pack_i64 = struct.Struct('<q').pack
_unpack_i64 = struct.Struct('<q').unpack_from
_pack_f64 = struct.Struct('<d').pack
_unpack_f64 = struct.Struct('<d').unpack_from

# Varint helpers (same scheme as codec.py, duplicated to avoid import coupling).
_pack_u16be = struct.Struct('>H').pack
_pack_u32be = struct.Struct('>I').pack
_unpack_u16be = struct.Struct('>H').unpack_from
_unpack_u32be = struct.Struct('>I').unpack_from


def _encode_varint(length):
    """Encode a non-negative integer as a 1–5 byte varint."""
    if length <= 0x7f:
        return bytes([length])
    if length <= 0xff:
        return b'\xcc' + bytes([length])
    if length <= 0xffff:
        return b'\xcd' + _pack_u16be(length)
    if length <= 0xffffffff:
        return b'\xce' + _pack_u32be(length)
    raise ValueError("field length exceeds 2^32-1: " + str(length))


def _decode_varint(buf, offset):
    """Return (value, bytes_consumed). Raises ValueError on truncated/unknown tags."""
    if offset >= len(buf):
        raise ValueError("truncated buffer: cannot read varint tag at offset " + str(offset))
    tag = buf[offset]
    if tag <= 0x7f:
        return tag, 1
    if tag == 0xcc:
        if offset + 2 > len(buf):
            raise ValueError("truncated buffer: varint 0xcc at offset " + str(offset))
        return buf[offset + 1], 2
    if tag == 0xcd:
        if offset + 3 > len(buf):
            raise ValueError("truncated buffer: varint 0xcd at offset " + str(offset))
        return _unpack_u16be(buf, offset + 1)[0], 3
    if tag == 0xce:
        if offset + 5 > len(buf):
            raise ValueError("truncated buffer: varint 0xce at offset " + str(offset))
        return _unpack_u32be(buf, offset + 1)[0], 5
    raise ValueError("unknown varint tag: " + hex(tag))


def _encode_field(f):
    """Encode a single field to bytes."""
    if type(f) is int:
        return b'i' + _pack_i64(f)
    if type(f) is float:
        return b'f' + _pack_f64(f)
    if type(f) is bytes:
        return b'b' + _encode_varint(len(f)) + f
    # Default: coerce to string.
    b = str(f).encode('utf-8')
    return b's' + _encode_varint(len(b)) + b


def _decode_field(buf, offset):
    """Decode a single field starting at *offset*.

    Returns (value, new_offset).
    Raises ValueError on truncated or malformed buffers.
    """
    if offset >= len(buf):
        raise ValueError("truncated buffer: cannot read type tag at offset " + str(offset))
    t = buf[offset]
    offset += 1

    if t == 0x69:  # 'i'
        if offset + 8 > len(buf):
            raise ValueError("truncated buffer: int64 at offset " + str(offset))
        return _unpack_i64(buf, offset)[0], offset + 8

    if t == 0x66:  # 'f'
        if offset + 8 > len(buf):
            raise ValueError("truncated buffer: float64 at offset " + str(offset))
        return _unpack_f64(buf, offset)[0], offset + 8

    # Variable-length: read varint, then payload.
    length, vsize = _decode_varint(buf, offset)
    offset += vsize
    end = offset + length
    if end > len(buf):
        raise ValueError(
            "truncated buffer: payload needs " + str(length)
            + " bytes at offset " + str(offset)
            + " but buffer has " + str(len(buf))
        )

    if t == 0x62:  # 'b'
        return buf[offset:end], end
    # 's' (0x73) or unknown — decode as UTF-8 string.
    return buf[offset:end].decode('utf-8'), end


def packb(key, value):
    """Serialize a (key, value) pair to bytes."""
    return _encode_field(key) + _encode_field(value)


def unpackb(buf):
    """Deserialize bytes to a (key, value) pair."""
    key, offset = _decode_field(buf, 0)
    value, _ = _decode_field(buf, offset)
    return key, value
