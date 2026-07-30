# River POC example

Runs the hello-world kubernetes adapter with events delivered through the
broker library's `river` backend: Postgres is the queue, no RabbitMQ or
Pub/Sub in the path. The task config and Job manifest are copies of the
`kubernetes` example, the only change is the broker block in `values.yaml`
(raw `broker.yaml` passthrough with a Postgres DSN).

Install:

```bash
helm install adapter ./charts -f charts/examples/river-poc/values.yaml
```

Requirements: a reachable Postgres (the kind pipeline deploys one as
`river-postgres`), and a Sentinel publishing with `broker.type=river`
against the same database. The subscription registry row for this adapter
is created automatically on startup.

Observe the queue directly:

```bash
psql "$DSN" -c "SELECT queue, state, count(*) FROM river_job GROUP BY 1,2 ORDER BY 1"
psql "$DSN" -c "TABLE broker_subscriptions"
```
