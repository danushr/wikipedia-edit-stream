import argparse
import time

from pyspark.sql import functions as F

from stream import paths
from stream.spark import get_spark
from stream.transform import parse, per_minute

SILVER_WATERMARK = "10 minutes"
GOLD_WATERMARK = "2 minutes"


def _start(df, lake, cks, name, once, mode="append"):
    w = (
        df.writeStream.format("delta")
        .outputMode(mode)
        .option("checkpointLocation", str(cks / name))
        .queryName(name)
    )
    w = w.trigger(availableNow=True) if once else w.trigger(processingTime="10 seconds")
    q = w.start(str(lake / name))
    print(f"started {name}", flush=True)
    return q


def _wait_for_first_commit(lake, name):
    first = lake / name / "_delta_log" / "00000000000000000000.json"
    while not first.exists():
        time.sleep(1)


def run(spark, bootstrap, topic, lake=paths.LAKE_DIR, cks=paths.CHECKPOINT_DIR, once=False):
    kafka = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 50_000)
        .load()
    )
    bronze = kafka.select(
        F.col("value").cast("string").alias("value"),
        F.col("partition"),
        F.col("offset"),
        F.col("timestamp").alias("kafka_time"),
    )
    q = [_start(bronze, lake, cks, "bronze_events", once)]
    if once:
        q[0].awaitTermination()
        print("bronze done", flush=True)

    _wait_for_first_commit(lake, "bronze_events")
    raw = spark.readStream.format("delta").load(str(lake / "bronze_events")).select("value")
    good, bad = parse(raw)
    silver = good.withWatermark("event_time", SILVER_WATERMARK).dropDuplicatesWithinWatermark(["event_id"])
    q += [_start(silver, lake, cks, "silver_events", once), _start(bad, lake, cks, "dead_letter", once)]
    if once:
        for x in q[1:]:
            x.awaitTermination()
        print("silver and dead letter done", flush=True)

    _wait_for_first_commit(lake, "silver_events")
    events = spark.readStream.format("delta").load(str(lake / "silver_events"))
    gold = per_minute(events.withWatermark("event_time", GOLD_WATERMARK))
    q.append(_start(gold, lake, cks, "gold_edits_per_minute", once))
    return q


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true", help="process what is in Kafka now, then stop")
    a = ap.parse_args()
    spark = get_spark()
    queries = run(spark, paths.KAFKA_BOOTSTRAP, paths.KAFKA_TOPIC, once=a.once)
    for x in queries:
        if a.once and not x.awaitTermination(90):
            print("stuck:", x.name, flush=True)
            spark.stop()
            raise SystemExit(1)
        x.awaitTermination()
