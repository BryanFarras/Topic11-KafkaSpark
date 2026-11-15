#!/usr/bin/env python3
"""
Milestone 1 Verification Script
Validates Kafka broker connectivity, topic partitions, producer output,
JSON schema integrity, timestamp formatting, and per-user partition affinity.
"""

import json
import sys
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient

BOOTSTRAP_SERVER = "localhost:9092"
TOPIC = "klik"
VALID_CATEGORIES = {"fashion", "elektronik", "rumah", "olahraga", "buku"}


def verify_broker_and_topic():
    print(f"[M1 Verification] Checking broker and topic '{TOPIC}'...")
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVER})
    metadata = admin.list_topics(timeout=5)

    if TOPIC not in metadata.topics:
        print(f"FAILED: Topic '{TOPIC}' not found in cluster!")
        sys.exit(1)

    topic_metadata = metadata.topics[TOPIC]
    partition_count = len(topic_metadata.partitions)
    print(f"Topic '{TOPIC}' found with {partition_count} partitions.")

    if partition_count != 3:
        print(f"FAILED: Expected 3 partitions, found {partition_count}!")
        sys.exit(1)

    print("Topic metadata check PASSED.")


def verify_messages():
    print(f"[M1 Verification] Consuming sample messages from '{TOPIC}' across all partitions...")
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVER,
        "group.id": "m1-verifier-group-unique",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })

    user_partition_map = {}
    partition_hits = set()
    total_samples = 0

    try:
        # Check each of the 3 partitions explicitly
        for p_id in range(3):
            tp = TopicPartition(TOPIC, p_id, 0)
            low, high = consumer.get_watermark_offsets(tp, timeout=5.0)
            print(f"Partition {p_id} offset range: [{low}, {high}] (total messages: {high - low})")
            assert high > 0, f"Partition {p_id} has no messages!"
            
            consumer.assign([tp])
            p_samples = 0
            retries = 0
            while p_samples < 20 and retries < 5:
                msg = consumer.poll(1.0)
                if msg is None:
                    retries += 1
                    continue
                if msg.error():
                    print(f"Consumer error: {msg.error()}")
                    break

                key_bytes = msg.key()
                val_bytes = msg.value()
                partition = msg.partition()
                assert partition == p_id
                partition_hits.add(partition)

                # Check key
                assert key_bytes is not None, "Message key is None!"
                key_str = key_bytes.decode("utf-8")

                # Check JSON schema
                val = json.loads(val_bytes.decode("utf-8"))
                assert "user_id" in val, "Missing user_id field"
                assert "kategori" in val, "Missing kategori field"
                assert "waktu" in val, "Missing waktu field"

                assert isinstance(val["user_id"], int), "user_id must be integer"
                assert str(val["user_id"]) == key_str, f"Key {key_str} != user_id {val['user_id']}"
                assert val["kategori"] in VALID_CATEGORIES, f"Invalid kategori: {val['kategori']}"
                assert len(val["waktu"]) == 19, f"Invalid timestamp format: {val['waktu']}"

                # Check partition affinity
                if key_str in user_partition_map:
                    assert user_partition_map[key_str] == partition, (
                        f"Partition affinity violated! User {key_str} saw partition {user_partition_map[key_str]} "
                        f"and partition {partition}"
                    )
                else:
                    user_partition_map[key_str] = partition

                p_samples += 1
                total_samples += 1

    finally:
        consumer.close()

    print(f"Successfully validated {total_samples} messages across {len(partition_hits)} partitions.")
    print(f"Partitions observed: {sorted(list(partition_hits))}")

    if len(partition_hits) != 3:
        print(f"FAILED: Expected 3 partitions, got {len(partition_hits)}!")
        sys.exit(1)
    else:
        print("All 3 partitions actively verified with valid schemas and per-user affinity.")

    print("[M1 Verification] ALL CHECKS PASSED.")


if __name__ == "__main__":
    verify_broker_and_topic()
    verify_messages()
