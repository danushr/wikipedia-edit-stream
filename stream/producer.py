import argparse
import json
import time

import requests
from confluent_kafka import Producer

from stream import paths

URL = "https://stream.wikimedia.org/v2/stream/recentchange"
HEADERS = {"User-Agent": "wikipedia-edit-stream (github.com/danushr/wikipedia-edit-stream)"}


def events(lines):
    for line in lines:
        if isinstance(line, bytes):
            line = line.decode("utf-8", "replace")
        if line.startswith("data: "):
            yield line[6:]


def live():
    backoff = 1
    while True:
        try:
            with requests.get(URL, headers=HEADERS, stream=True, timeout=60) as r:
                r.raise_for_status()
                backoff = 1
                yield from events(r.iter_lines())
        except requests.RequestException as e:
            print(f"stream dropped ({e}), retrying in {backoff}s", flush=True)
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)


def send(values, bootstrap, topic, limit=None):
    p = Producer({"bootstrap.servers": bootstrap, "enable.idempotence": True, "linger.ms": 50})
    n = 0
    for v in values:
        key = (json.loads(v).get("wiki") or "").encode() if v.startswith("{") else b""
        p.produce(topic, v.encode(), key=key)
        p.poll(0)
        n += 1
        if limit and n >= limit:
            break
    p.flush()
    return n


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--replay", type=argparse.FileType(), help="send a saved file of events instead of the live stream")
    ap.add_argument("--limit", type=int)
    a = ap.parse_args()
    src = (l.rstrip("\n") for l in a.replay) if a.replay else live()
    print("sent", send(src, paths.KAFKA_BOOTSTRAP, paths.KAFKA_TOPIC, a.limit))
