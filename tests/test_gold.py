from datetime import datetime, timezone

from stream.transform import per_minute

COLS = "event_id string, event_time timestamp, wiki string, is_bot boolean, bytes_changed int"


def t(minute, sec):
    return datetime(2026, 11, 12, 19, minute, sec, tzinfo=timezone.utc)


def test_edits_are_counted_per_wiki_per_minute_with_bot_count_and_net_bytes(spark):
    df = spark.createDataFrame(
        [
            ("a", t(0, 5), "enwiki", False, 10),
            ("b", t(0, 50), "enwiki", True, -4),
            ("c", t(0, 10), "dewiki", False, None),
            ("d", t(1, 5), "enwiki", False, 7),
        ],
        COLS,
    )
    rows = {
        (r.wiki, r.window_start.astimezone(timezone.utc).strftime("%H:%M")): r
        for r in per_minute(df).collect()
    }
    assert set(rows) == {("enwiki", "19:00"), ("dewiki", "19:00"), ("enwiki", "19:01")}
    assert (rows[("enwiki", "19:00")].edits, rows[("enwiki", "19:00")].bot_edits) == (2, 1)
    assert rows[("enwiki", "19:00")].bytes_changed == 6
    assert rows[("dewiki", "19:00")].bytes_changed == 0
    assert rows[("enwiki", "19:01")].edits == 1
