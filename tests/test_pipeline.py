import socket
import uuid

import pytest

from stream import paths
from stream.producer import send
from stream.run import run

host, port = paths.KAFKA_BOOTSTRAP.split(":")


def broker_up():
    try:
        socket.create_connection((host, int(port)), timeout=2).close()
        return True
    except OSError:
        return False


pytestmark = [
    pytest.mark.kafka,
    pytest.mark.skipif(not broker_up(), reason="no Kafka broker at KAFKA_BOOTSTRAP"),
]


def test_duplicates_collapse_and_bad_records_are_set_aside(spark, tmp_path):
    lines = paths.SAMPLE.read_text().splitlines()[:500]
    unique_ids = {l.split('"id":"', 1)[1].split('"', 1)[0] for l in lines}
    sent = lines + lines[:40] + ["not json {", "{}", '{"meta":{"id":"x","dt":"never"}}']
    topic = f"test-{uuid.uuid4().hex[:8]}"
    send(sent, paths.KAFKA_BOOTSTRAP, topic)

    for q in run(spark, paths.KAFKA_BOOTSTRAP, topic, tmp_path / "lake", tmp_path / "ck", once=True):
        q.awaitTermination()

    count = lambda t: spark.read.format("delta").load(str(tmp_path / "lake" / t)).count()
    assert count("bronze_events") == len(sent)
    assert count("silver_events") == len(unique_ids)
    assert count("dead_letter") == 3
