import tempfile
import unittest
from pathlib import Path

from orders import Conflict, apply, connect, dispatch


class OrderTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = connect(str(Path(self.tmp.name) / "orders.db"))

    def tearDown(self):
        self.db.close()
        self.tmp.cleanup()

    def test_idempotent_commands_and_version(self):
        first = apply(self.db, "cmd1", "o1", "create", amount_cents=1000, now=1)
        self.assertEqual(first, apply(self.db, "cmd1", "o1", "create", amount_cents=9999, now=2))
        paid = apply(self.db, "cmd2", "o1", "paid", expected_version=1, now=3)
        self.assertEqual(paid["version"], 2)
        with self.assertRaises(Conflict):
            apply(self.db, "cmd3", "o1", "fulfilled", expected_version=1)
        self.assertEqual(self.db.execute("SELECT count(*) FROM outbox").fetchone()[0], 2)

    def test_invalid_transition_rolls_back(self):
        apply(self.db, "one", "o1", "create", amount_cents=100)
        with self.assertRaises(Conflict):
            apply(self.db, "two", "o1", "fulfilled")
        self.assertEqual(self.db.execute("SELECT status FROM orders").fetchone()[0], "created")
        self.assertEqual(self.db.execute("SELECT count(*) FROM commands").fetchone()[0], 1)

    def test_outbox_replays_after_failure(self):
        apply(self.db, "one", "o1", "create", amount_cents=100)
        def broken(event):
            raise RuntimeError("broker unavailable")
        with self.assertRaises(RuntimeError):
            dispatch(self.db, broken)
        events = []
        self.assertEqual(dispatch(self.db, events.append), 1)
        self.assertEqual(events[0]["type"], "order.created")
        self.assertEqual(dispatch(self.db, events.append), 0)

