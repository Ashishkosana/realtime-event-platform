from __future__ import annotations

import argparse
import json
import sys

import httpx

from notify_platform.config import load_settings


def _base() -> str:
    s = load_settings()
    host = "127.0.0.1" if s.api_host in {"0.0.0.0", "::"} else s.api_host
    return f"http://{host}:{s.api_port}"


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="notify")
    sub = parser.add_subparsers(dest="cmd", required=True)
    pub = sub.add_parser("publish")
    pub.add_argument("--tenant", default="acme")
    pub.add_argument("--type", dest="event_type", default="order.paid")
    pub.add_argument("--key", required=True)
    pub.add_argument("--payload", default="{}")
    pub.add_argument("--to", required=True, help="comma-separated user ids")
    inbox = sub.add_parser("inbox")
    inbox.add_argument("--tenant", default="acme")
    inbox.add_argument("--user", required=True)
    mute = sub.add_parser("mute")
    mute.add_argument("--tenant", default="acme")
    mute.add_argument("--user", required=True)
    mute.add_argument("--type", dest="event_type", required=True)
    mute.add_argument("--off", action="store_true")
    sub.add_parser("health")
    args = parser.parse_args(argv)
    with httpx.Client(base_url=_base(), timeout=15.0) as client:
        if args.cmd == "publish":
            resp = client.post(
                "/events",
                json={
                    "tenant": args.tenant,
                    "event_type": args.event_type,
                    "idempotency_key": args.key,
                    "payload": json.loads(args.payload),
                    "recipients": [x.strip() for x in args.to.split(",") if x.strip()],
                },
            )
        elif args.cmd == "inbox":
            resp = client.get("/inbox", params={"tenant": args.tenant, "user_id": args.user})
        elif args.cmd == "mute":
            resp = client.post(
                "/prefs/mute",
                json={
                    "tenant": args.tenant,
                    "user_id": args.user,
                    "event_type": args.event_type,
                    "muted": not args.off,
                },
            )
        else:
            resp = client.get("/health")
        sys.stdout.write(json.dumps(resp.json(), indent=2, default=str) + "\n")
        if resp.status_code >= 400:
            raise SystemExit(1)


if __name__ == "__main__":
    main()
