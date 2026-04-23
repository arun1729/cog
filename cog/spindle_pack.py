"""Custom binary packer for Spindle (key, value) payloads.

Replaces msgpack entirely. Key and Value will always be string or numbers.
"""
import struct

_pack_u32 = struct.Struct('>I').pack
_unpack_u32_from = struct.Struct('>I').unpack_from


def _encode_field(f):
    if type(f) is int:
        b = str(f).encode('utf-8')
        t = b'i'
    elif type(f) is float:
        b = str(f).encode('utf-8')
        t = b'f'
    elif type(f) is bytes:
        b = f
        t = b'b'
    else:
        b = str(f).encode('utf-8')
        t = b's'
    
    klen = len(b)
    return t + _pack_u32(klen) + b


def _decode_field(buf, offset):
    t = buf[offset:offset+1]
    length = _unpack_u32_from(buf, offset+1)[0]
    start = offset + 5
    end = start + length
    b = buf[start:end]
    
    if t == b'i':
        return int(b.decode('utf-8')), end
    elif t == b'f':
        return float(b.decode('utf-8')), end
    elif t == b'b':
        return b, end
    else:
        return b.decode('utf-8'), end


def packb(key, value):
    return _encode_field(key) + _encode_field(value)


def unpackb(buf):
    key, offset = _decode_field(buf, 0)
    value, _ = _decode_field(buf, offset)
    return key, value
