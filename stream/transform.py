from pyspark.sql import functions as F

SCHEMA = """
  meta struct<id:string, dt:string, domain:string>,
  type string, wiki string, user string, bot boolean, title string,
  namespace int, length struct<old:int, new:int>
"""


def parse(raw):
    p = raw.withColumn("e", F.from_json("value", SCHEMA)).withColumn("ts", F.try_to_timestamp("e.meta.dt"))
    reason = (
        F.when(F.try_parse_json("value").isNull(), "invalid json")
        .when(F.col("e.meta.id").isNull(), "missing event id")
        .when(F.col("ts").isNull(), "missing event time")
    )
    p = p.withColumn("reason", reason)
    good = p.where(F.col("reason").isNull()).select(
        F.col("e.meta.id").alias("event_id"),
        F.col("ts").alias("event_time"),
        F.col("e.wiki").alias("wiki"),
        F.col("e.type").alias("type"),
        F.col("e.user").alias("user"),
        F.coalesce("e.bot", F.lit(False)).alias("is_bot"),
        F.col("e.title").alias("title"),
        F.col("e.namespace").alias("namespace"),
        (F.col("e.length.new") - F.col("e.length.old")).alias("bytes_changed"),
    )
    bad = p.where(F.col("reason").isNotNull()).select("value", "reason")
    return good, bad
