"""Unit tests for cog.spindle_pack — the foundation serializer for the v4
on-disk format. Every byte CogDB persists flows through packb/unpackb, so this
exercises every type, every varint boundary, and the truncation/validation
error paths directly (rather than only indirectly via Store)."""

import math
import struct
import unittest

from cog.spindle_pack import (
    packb, unpackb, _encode_varint, _decode_varint,
)


def _roundtrip(key, value):
    return unpackb(packb(key, value))


class TestSpindlePackRoundtrip(unittest.TestCase):

    def test_string(self):
        self.assertEqual(_roundtrip("alice", "bob"), ("alice", "bob"))

    def test_empty_string(self):
        self.assertEqual(_roundtrip("", ""), ("", ""))

    def test_unicode(self):
        self.assertEqual(_roundtrip("naïve", "café—测试🚀"), ("naïve", "café—测试🚀"))

    def test_sentinel_bytes_in_string(self):
        # 0xFD / 0xAC were legacy record/unit separators; the v4 format is
        # length-addressable so these must survive as ordinary content.
        s = "a\xfd\xacb"
        self.assertEqual(_roundtrip(s, s), (s, s))

    def test_bytes_key(self):
        # Graph edge keys are bytes (b'\x00' + vertex) in v4 — must round-trip.
        self.assertEqual(_roundtrip(b"\x00alice", "bob"), (b"\x00alice", "bob"))

    def test_empty_bytes(self):
        self.assertEqual(_roundtrip(b"", b""), (b"", b""))

    def test_bytes_with_sentinels(self):
        b = b"\x00\xfd\xac\xff"
        self.assertEqual(_roundtrip(b, b), (b, b))

    def test_bool(self):
        self.assertEqual(_roundtrip("k", True), ("k", True))
        self.assertEqual(_roundtrip("k", False), ("k", False))

    def test_bool_not_confused_with_int(self):
        # bool is a subclass of int; the decoded value must stay a real bool.
        _, v = _roundtrip("k", True)
        self.assertIs(v, True)
        self.assertIsInstance(v, bool)

    def test_int_zero_and_small(self):
        for n in (0, 1, -1, 127, 128, 255, 256, 65535, 65536):
            self.assertEqual(_roundtrip("k", n), ("k", n))

    def test_int_negative(self):
        self.assertEqual(_roundtrip("k", -987654321), ("k", -987654321))

    def test_int64_boundaries(self):
        for n in (2**63 - 1, -(2**63)):
            self.assertEqual(_roundtrip("k", n), ("k", n))

    def test_int_out_of_range_raises(self):
        with self.assertRaises(ValueError):
            packb("k", 2**63)
        with self.assertRaises(ValueError):
            packb("k", -(2**63) - 1)

    def test_float(self):
        for f in (0.0, -0.0, 3.14159, -2.5e300, 1e-300):
            _, v = _roundtrip("k", f)
            self.assertEqual(v, f)

    def test_float_inf(self):
        _, v = _roundtrip("k", float("inf"))
        self.assertTrue(math.isinf(v) and v > 0)

    def test_float_nan(self):
        _, v = _roundtrip("k", float("nan"))
        self.assertTrue(math.isnan(v))

    def test_list_of_floats(self):
        _, v = _roundtrip("k", [1.5, 2.5, 3.5])
        self.assertEqual(v, [1.5, 2.5, 3.5])

    def test_list_of_ints_becomes_floats(self):
        # Lists are encoded as a float64 array — ints widen to float.
        _, v = _roundtrip("embedding", [1, 2, 3])
        self.assertEqual(v, [1.0, 2.0, 3.0])
        self.assertTrue(all(isinstance(x, float) for x in v))

    def test_empty_list(self):
        self.assertEqual(_roundtrip("k", []), ("k", []))

    def test_large_float_array(self):
        arr = [float(i) * 0.5 for i in range(1000)]
        _, v = _roundtrip("vec", arr)
        self.assertEqual(v, arr)

    def test_list_with_non_numeric_raises(self):
        with self.assertRaises(ValueError):
            packb("k", [1.0, "x", 2.0])

    def test_unknown_type_falls_back_to_str(self):
        # Documented fallback: unsupported value types are str()'d.
        self.assertEqual(_roundtrip("k", None), ("k", "None"))


class TestSpindlePackInterning(unittest.TestCase):

    def test_decoded_strings_are_interned(self):
        # Identical decoded strings should share one object so bucket-walk
        # equality checks degrade to pointer comparisons.
        k1, _ = unpackb(packb("shared_key", "v1"))
        k2, _ = unpackb(packb("shared_key", "v2"))
        self.assertIs(k1, k2)


class TestVarint(unittest.TestCase):
    """Direct coverage of the 1/2/3/5-byte varint boundaries."""

    def _rt(self, n):
        buf = _encode_varint(n)
        value, consumed = _decode_varint(buf, 0)
        self.assertEqual(value, n)
        self.assertEqual(consumed, len(buf))
        return len(buf)

    def test_one_byte_range(self):
        self.assertEqual(self._rt(0), 1)
        self.assertEqual(self._rt(0x7f), 1)

    def test_two_byte_range(self):
        self.assertEqual(self._rt(0x80), 2)
        self.assertEqual(self._rt(0xff), 2)

    def test_three_byte_range(self):
        self.assertEqual(self._rt(0x100), 3)
        self.assertEqual(self._rt(0xffff), 3)

    def test_five_byte_range(self):
        self.assertEqual(self._rt(0x10000), 5)
        self.assertEqual(self._rt(0xffffffff), 5)

    def test_overflow_raises(self):
        with self.assertRaises(ValueError):
            _encode_varint(0x1_0000_0000)

    def test_decode_truncated_raises(self):
        # A 0xce tag claims 4 trailing bytes; supply fewer.
        with self.assertRaises(ValueError):
            _decode_varint(b"\xce\x01\x02", 0)

    def test_decode_unknown_tag_raises(self):
        with self.assertRaises(ValueError):
            _decode_varint(b"\xcf", 0)


class TestSpindlePackTruncation(unittest.TestCase):
    """A corrupted/truncated buffer must raise, never silently return garbage."""

    def test_truncated_string_payload(self):
        buf = packb("hello", "world")
        with self.assertRaises(ValueError):
            unpackb(buf[:-3])

    def test_truncated_int(self):
        buf = packb("k", 1234567890)
        with self.assertRaises((ValueError, struct.error)):
            unpackb(buf[:-2])

    def test_truncated_float_array(self):
        buf = packb("vec", [1.0, 2.0, 3.0, 4.0])
        with self.assertRaises((ValueError, struct.error)):
            unpackb(buf[:-5])

    def test_empty_buffer(self):
        with self.assertRaises(ValueError):
            unpackb(b"")


if __name__ == "__main__":
    unittest.main()
