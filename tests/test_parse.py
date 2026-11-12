import json
from datetime import timezone

from stream.transform import parse

EDIT = {
    "meta": {"id": "e1", "dt": "2026-11-12T19:59:21.090Z", "domain": "en.wikipedia.org"},
    "type": "edit", "wiki": "enwiki", "user": "Alice", "bot": False,
    "title": "Cat", "namespace": 0, "length": {"old": 100, "new": 130},
}
LOG = {
    "meta": {"id": "e2", "dt": "2026-11-12T19:59:22.000Z", "domain": "commons.wikimedia.org"},
    "type": "log", "wiki": "commonswiki", "user": "Bob", "bot": True,
    "title": "Dog", "namespace": 6,
}


def raw(spark, *values):
    return spark.createDataFrame([(v if isinstance(v, str) else json.dumps(v),) for v in values], "value string")


def test_good_rows_get_typed_columns_and_bytes_changed(spark):
    good, _ = parse(raw(spark, EDIT, LOG))
    rows = {r.event_id: r for r in good.collect()}
    assert rows["e1"].bytes_changed == 30
    assert rows["e1"].wiki == "enwiki" and rows["e1"].is_bot is False
    assert rows["e1"].event_time.astimezone(timezone.utc).isoformat() == "2026-11-12T19:59:21.090000+00:00"
    assert rows["e2"].bytes_changed is None and rows["e2"].is_bot is True


def test_garbage_and_missing_ids_go_to_dead_letter_with_a_reason(spark):
    no_id = {**EDIT, "meta": {"dt": "2026-11-12T19:59:21.090Z"}}
    no_time = {**EDIT, "meta": {"id": "e3", "dt": "yesterday"}}
    good, bad = parse(raw(spark, EDIT, "not json {", no_id, no_time))
    assert good.count() == 1
    reasons = {r.value: r.reason for r in bad.collect()}
    assert reasons["not json {"] == "invalid json"
    assert sorted(reasons.values()) == ["invalid json", "missing event id", "missing event time"]
