"""Regression test: reopening a graph and then closing, syncing, truncating or
dropping it must not fail with OSError [Errno 22] on Windows.

On Windows, CPython's mmap constructor (Modules/mmapmodule.c) seeks the
descriptor it is given back to offset 0 before mapping. When the store mapped
the descriptor of its own buffered writer, that seek moved the OS file
position behind the buffered object's back. After a reopen, the header read
leaves read-ahead in the buffer, so the next flush() or close() tried to
rewind to a negative offset and failed with OSError [Errno 22].

These tests emulate that seek on every platform by wrapping mmap.mmap, so the
bug is caught by CI on Linux and macOS too.
"""

import mmap
import os
import shutil
import unittest
from unittest import mock

from cog.torque import Graph

DB_PATH = "/tmp/TestStoreMmapWindowsSeek"
GRAPH = "people"

_real_mmap = mmap.mmap


def _windows_mmap(fileno, *args, **kwargs):
    """mmap.mmap with CPython-on-Windows behaviour: seek the fd to 0 first."""
    if fileno not in (-1, 0):
        os.lseek(fileno, 0, os.SEEK_SET)
    return _real_mmap(fileno, *args, **kwargs)


class StoreMmapWindowsSeekTest(unittest.TestCase):

    def setUp(self):
        shutil.rmtree(DB_PATH, ignore_errors=True)
        g = Graph(GRAPH, cog_path_prefix=DB_PATH)
        g.put("alice", "knows", "bob")
        g.put("bob", "knows", "carol")
        g.put("alice", "age", "30")
        g.close()
        # Everything after setup runs with the Windows seek emulated.
        patcher = mock.patch.object(mmap, "mmap", _windows_mmap)
        patcher.start()
        self.addCleanup(patcher.stop)

    def tearDown(self):
        shutil.rmtree(DB_PATH, ignore_errors=True)

    def _open(self):
        return Graph(GRAPH, cog_path_prefix=DB_PATH)

    def test_close_after_reopen(self):
        g = self._open()
        g.close()

    def test_close_after_reopen_and_read(self):
        g = self._open()
        self.assertEqual(g.v("alice").out("knows").all(),
                         {'result': [{'id': 'bob'}]})
        g.close()

    def test_sync_after_reopen(self):
        g = self._open()
        g.sync()
        g.close()

    def test_truncate_after_reopen(self):
        g = self._open()
        g.truncate()
        self.assertEqual(g.v().count(), 0)
        g.put("dave", "knows", "erin")
        self.assertEqual(g.v("dave").out("knows").all(),
                         {'result': [{'id': 'erin'}]})
        g.close()

    def test_drop_after_reopen(self):
        g = self._open()
        graph_dir = g.config.cog_data_dir(GRAPH)
        self.assertTrue(os.path.exists(graph_dir))
        g.drop()
        self.assertFalse(os.path.exists(graph_dir))

    def test_data_intact_across_reopens(self):
        g = self._open()
        self.assertEqual(g.v("bob").out("knows").all(),
                         {'result': [{'id': 'carol'}]})
        g.put("carol", "knows", "dave")
        g.close()

        g = self._open()
        self.assertEqual(g.v("alice").out("knows").all(),
                         {'result': [{'id': 'bob'}]})
        self.assertEqual(g.v("alice").out("age").all(),
                         {'result': [{'id': '30'}]})
        self.assertEqual(g.v("carol").out("knows").all(),
                         {'result': [{'id': 'dave'}]})
        self.assertEqual(g.v("alice").out("knows").out("knows").out("knows").all(),
                         {'result': [{'id': 'dave'}]})
        g.close()


if __name__ == '__main__':
    unittest.main()
