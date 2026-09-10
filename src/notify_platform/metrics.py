from __future__ import annotations

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest

REGISTRY = CollectorRegistry()

PUBLISH = Counter(
    "ntf_publish_total",
    "Publish attempts",
    ["event_type", "duplicate"],
    registry=REGISTRY,
)
INBOX = Counter("ntf_inbox_total", "Inbox rows written", ["event_type"], registry=REGISTRY)
MUTED = Counter(
    "ntf_muted_total", "Fan-out skipped by preference", ["event_type"], registry=REGISTRY
)
DELIVER = Counter(
    "ntf_deliver_total",
    "Async delivery outcomes",
    ["channel", "outcome"],
    registry=REGISTRY,
)
CONNECTED = Gauge("ntf_sse_connected", "Open SSE streams", registry=REGISTRY)
PUBLISH_MS = Histogram(
    "ntf_publish_ms",
    "publish() latency",
    buckets=(1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000),
    registry=REGISTRY,
)


def render() -> bytes:
    return generate_latest(REGISTRY)
