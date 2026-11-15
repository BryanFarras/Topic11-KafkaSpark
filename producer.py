#!/usr/bin/env python3
"""
High-Throughput Clickstream Kafka Producer
Topic 11 — Kafka & PySpark Structured Streaming Ingestion Pipeline

Generates JSON clickstream events with schema:
  {"user_id": int, "kategori": str, "waktu": "YYYY-MM-DDTHH:MM:SS"}
Partitioned deterministically by user_id to ensure per-user FIFO order.
"""

import argparse
import json
import random
import sys
import time
from datetime import datetime, timezone
from confluent_kafka import Producer

KATEGORI_LIST = ["fashion", "elektronik", "rumah", "olahraga", "buku"]


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="High-Throughput Clickstream Kafka Producer"
    )
    # Support positional arguments: python producer.py [rate] [duration]
    parser.add_argument(
        "pos_rate",
        nargs="?",
        type=int,
        default=None,
        help="Positional argument: events per second (default: 100)",
    )
    parser.add_argument(
        "pos_duration",
        nargs="?",
        type=int,
        default=None,
        help="Positional argument: duration in seconds (default: 60)",
    )
    # Support flag arguments: python producer.py --rate 100 --duration 60
    parser.add_argument(
        "--rate",
        type=int,
        default=100,
        help="Events per second (default: 100)",
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=60,
        help="Duration in seconds (default: 60)",
    )
    parser.add_argument(
        "--bootstrap-server",
        default="localhost:9092",
        help="Kafka broker address (default: localhost:9092)",
    )
    parser.add_argument(
        "--topic",
        default="klik",
        help="Kafka topic name (default: klik)",
    )

    args = parser.parse_args()

    # Prioritize positional arguments if provided
    rate = args.pos_rate if args.pos_rate is not None else args.rate
    duration = args.pos_duration if args.pos_duration is not None else args.duration

    return rate, duration, args.bootstrap_server, args.topic


def run_producer():
    rate, duration, bootstrap_server, topic = parse_arguments()
    print(f"[Producer] Target Rate: {rate} msg/s | Duration: {duration}s | Broker: {bootstrap_server} | Topic: {topic}")

    conf = {
        "bootstrap.servers": bootstrap_server,
        "linger.ms": 20,
        "batch.size": 65536,
        "queue.buffering.max.messages": 500000,
        "acks": 1,
    }
    p = Producer(conf)

    sub_intervals = 10  # 10 intervals of 100ms per second
    tick_duration = 1.0 / sub_intervals
    total_ticks = int(duration * sub_intervals)

    start_time = time.time()
    total_sent = 0

    try:
        for tick in range(total_ticks):
            target_cumulative = ((tick + 1) * rate) // sub_intervals
            events_to_send = target_cumulative - total_sent

            for _ in range(events_to_send):
                user_id = random.randint(1, 5000)
                ev = {
                    "user_id": user_id,
                    "kategori": random.choice(KATEGORI_LIST),
                    "waktu": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"),
                }
                payload = json.dumps(ev).encode("utf-8")
                key_bytes = str(user_id).encode("utf-8")

                while True:
                    try:
                        p.produce(topic, key=key_bytes, value=payload)
                        break
                    except BufferError:
                        p.poll(0.05)

                total_sent += 1

            # Poll once per tick to process delivery reports and handle broker network I/O
            p.poll(0)

            # Sleep until the exact deadline for this sub-interval to eliminate timing drift
            target_time = start_time + (tick + 1) * tick_duration
            sleep_time = target_time - time.time()
            if sleep_time > 0:
                time.sleep(sleep_time)

    except KeyboardInterrupt:
        print("\n[Producer] Interrupted by user.")
    finally:
        print(f"[Producer] Flushing remaining messages...")
        p.flush(timeout=10)
        actual_duration = round(time.time() - start_time, 2)
        effective_rate = round(total_sent / actual_duration, 1) if actual_duration > 0 else 0
        print(f"terkirim: {total_sent} dalam {actual_duration} detik (efektif {effective_rate} msg/s)")


if __name__ == "__main__":
    run_producer()
