"""Custom binary packer for Spindle (key, value) payloads.

Replaces msgpack on the hot path. Payloads are framed by the outer Spindle
record's value_len varint, so `unpackb` is length-addressable: the value on
the fast path runs from (1 + klen_bytes + klen) to the end of the input
buffer.

Wire format (first byte is the tag):

    0x00..0x7f  (str, str), tag byte IS klen (0..127 bytes)
                [tag=klen][key utf-8][value utf-8]

    0x80        (str, str), klen as uint16 big-endian (128..65535 bytes)
                [0x80][klen u16 BE][key utf-8][value utf-8]

    0x81        (str, str), klen as uint32 big-endian
                [0x81][klen u32 BE][key utf-8][value utf-8]

    0xff        fallback: payload from byte 1 is raw msgpack
                [0xff][msgpack bytes ...]

Compared to msgpack.packb((key, value)):
    - saves 2-3 bytes per (str, str) record (no array header, no string headers)
    - avoids msgpack's per-element type dispatch on the write path
    - pure Python, no C extension dependency on the fast path

Tags 0x82..0xfe are reserved for future type-pair fast paths (e.g. a
str + float32[] path for embeddings).
"""
import struct

import msgpack

_TAG_STRSTR_U16 = 0x80
_TAG_STRSTR_U32 = 0x81
_TAG_MSGPACK = 0xFF

_pack_u16 = struct.Struct('>H').pack
_pack_u32 = struct.Struct('>I').pack
_unpack_u16_from = struct.Struct('>H').unpack_from
_unpack_u32_from = struct.Struct('>I').unpack_from


def packb(key, value):
    if type(key) is str and type(value) is str:
        k = key.encode('utf-8')
        v = value.encode('utf-8')
        klen = len(k)
        if klen <= 0x7f:
            return bytes((klen,)) + k + v
        if klen <= 0xffff:
            return b'\x80' + _pack_u16(klen) + k + v
        if klen <= 0xffffffff:
            return b'\x81' + _pack_u32(klen) + k + v
        raise ValueError("key too long: %d bytes" % klen)
    return b'\xff' + msgpack.packb((key, value), use_bin_type=True)


def unpackb(buf):
    tag = buf[0]
    if tag <= 0x7f:
        klen = tag
        kstart = 1
    elif tag == _TAG_STRSTR_U16:
        klen = _unpack_u16_from(buf, 1)[0]
        kstart = 3
    elif tag == _TAG_STRSTR_U32:
        klen = _unpack_u32_from(buf, 1)[0]
        kstart = 5
    elif tag == _TAG_MSGPACK:
        return msgpack.unpackb(buf[1:], raw=False)
    else:
        raise ValueError("unknown spindle_pack tag: " + hex(tag))
    kend = kstart + klen
    return buf[kstart:kend].decode('utf-8'), buf[kend:].decode('utf-8')
