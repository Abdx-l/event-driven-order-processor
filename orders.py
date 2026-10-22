"""Transactional order state machine with a durable event outbox."""
import argparse
import json
import sqlite3
import time


SCHEMA = """
CREATE TABLE IF NOT EXISTS orders (
 id TEXT PRIMARY KEY, amount_cents INTEGER NOT NULL,
 status TEXT NOT NULL, version INTEGER NOT NULL, created_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS commands (
 id TEXT PRIMARY KEY, order_id TEXT NOT NULL, result TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS outbox (
 id INTEGER PRIMARY KEY, order_id TEXT NOT NULL, event_type TEXT NOT NULL,
 payload TEXT NOT NULL, created_at REAL NOT NULL, delivered_at REAL
);
CREATE INDEX IF NOT EXISTS undelivered ON outbox(delivered_at,id);
"""

TRANSITIONS = {"created": {"paid", "cancelled"}, "paid": {"fulfilled", "cancelled"}, "fulfilled": set(), "cancelled": set()}


class Conflict(Exception):
    pass


def connect(path):
    db = sqlite3.connect(path, timeout=30, isolation_level=None)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA busy_timeout=30000")
    db.executescript(SCHEMA)
    return db


def apply(db, command_id, order_id, action, amount_cents=None, expected_version=None, now=None):
    """Atomically update state and append an event, or return a prior command result."""
    now = time.time() if now is None else now
    if not command_id or not order_id:
        raise ValueError("command_id and order_id are required")
    db.execute("BEGIN IMMEDIATE")
    try:
        previous = db.execute("SELECT order_id,result FROM commands WHERE id=?", (command_id,)).fetchone()
        if previous:
            if previous["order_id"] != order_id:
                raise Conflict("command ID belongs to another order")
            db.commit()
            return json.loads(previous["result"])
        old = db.execute("SELECT * FROM orders WHERE id=?", (order_id,)).fetchone()
        if action == "create":
            if old:
                raise Conflict("order already exists")
            if not isinstance(amount_cents, int) or isinstance(amount_cents, bool) or amount_cents <= 0:
                raise ValueError("positive integer amount_cents required")
            db.execute("INSERT INTO orders VALUES(?,?,?,?,?)", (order_id, amount_cents, "created", 1, now))
            result = {"id": order_id, "status": "created", "version": 1, "amount_cents": amount_cents}
        else:
            if not old:
                raise Conflict("order does not exist")
            if expected_version is not None and old["version"] != expected_version:
                raise Conflict(f"stale version: expected {expected_version}, actual {old['version']}")
            if action not in TRANSITIONS[old["status"]]:
                raise Conflict(f"cannot transition {old['status']} to {action}")
            version = old["version"] + 1
            db.execute("UPDATE orders SET status=?,version=? WHERE id=?", (action, version, order_id))
            result = {"id": order_id, "status": action, "version": version, "amount_cents": old["amount_cents"]}
        db.execute("INSERT INTO outbox(order_id,event_type,payload,created_at) VALUES(?,?,?,?)",
                   (order_id, f"order.{result['status']}", json.dumps(result, sort_keys=True), now))
        db.execute("INSERT INTO commands(id,order_id,result) VALUES(?,?,?)", (command_id, order_id, json.dumps(result)))
        db.commit()
        return result
    except BaseException:
        db.rollback()
        raise


def dispatch(db, publisher, limit=100, now=None):
    """Publish in sequence. A failed publish leaves the row available for replay."""
    now = time.time() if now is None else now
    sent = 0
    rows = db.execute("SELECT * FROM outbox WHERE delivered_at IS NULL ORDER BY id LIMIT ?", (limit,)).fetchall()
    for row in rows:
        event = {"event_id": row["id"], "order_id": row["order_id"], "type": row["event_type"], "payload": json.loads(row["payload"])}
        publisher(event)
        db.execute("UPDATE outbox SET delivered_at=? WHERE id=? AND delivered_at IS NULL", (now, row["id"]))
        sent += 1
    return sent


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="orders.db")
    sub = parser.add_subparsers(dest="command", required=True)
    create = sub.add_parser("create")
    create.add_argument("order_id")
    create.add_argument("amount", type=float, help="Amount in dollars")
    create.add_argument("--command-id", required=True)
    move = sub.add_parser("transition")
    move.add_argument("order_id")
    move.add_argument("status", choices=["paid", "fulfilled", "cancelled"])
    move.add_argument("--expected-version", type=int)
    move.add_argument("--command-id", required=True)
    sub.add_parser("dispatch")
    show = sub.add_parser("show")
    show.add_argument("order_id")
    args = parser.parse_args()
    with connect(args.db) as db:
        if args.command == "create":
            from decimal import Decimal
            cents = int(Decimal(str(args.amount)) * 100)
            result = apply(db, args.command_id, args.order_id, "create", amount_cents=cents)
        elif args.command == "transition":
            result = apply(db, args.command_id, args.order_id, args.status, expected_version=args.expected_version)
        elif args.command == "dispatch":
            result = {"sent": dispatch(db, lambda event: print(json.dumps(event)))}
        else:
            row = db.execute("SELECT * FROM orders WHERE id=?", (args.order_id,)).fetchone()
            result = dict(row) if row else None
        print(json.dumps(result))


if __name__ == "__main__":
    main()

