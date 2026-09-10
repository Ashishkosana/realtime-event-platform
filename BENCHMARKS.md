# Benchmarks

Measured on this agent VM (Python 3.12.3, Postgres 16.15). Not a capacity rating.

```bash
export DATABASE_URL=postgresql://workflow:workflow@127.0.0.1:5432/notify
python scripts/bench.py
```

| Bench | Result |
| --- | --- |
| 50 publishes, 1 recipient | **449 publishes/s**, 0.11 s total |
| 1 publish × 200 recipients (sync V1 path, `FANOUT_CHUNK=200`) | **0.15 s** to durable inbox |

Larger N uses V2 chunked fan-out (`ntf_fanout` + `SKIP LOCKED`). Kafka was not added.
