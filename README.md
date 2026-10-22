# Event-Driven Order Processor

An order state machine backed by SQLite transactions. Each accepted command updates the order and writes an event to a durable outbox in the same transaction. Command IDs provide idempotent retries, optimistic version checks reject stale updates, and an outbox dispatcher can replay undelivered events after a restart.

```sh
python orders.py create order-100 49.95 --command-id c1
python orders.py transition order-100 paid --expected-version 1 --command-id c2
python orders.py dispatch
python orders.py show order-100
python -m unittest discover -s tests -v
```

Allowed flow: `created -> paid -> fulfilled`, with cancellation from `created` or `paid`. The dispatch command prints undelivered events as JSON lines and marks them delivered. In a real integration, replace the print publisher with a broker client and preserve its idempotent consumption contract. Delivery is at least once if a process crashes between publishing and marking the row.

