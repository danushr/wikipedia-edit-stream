import os
from pathlib import Path

PROJECT_DIR = Path(os.environ["STREAM_PROJECT_DIR"])
DATA_DIR = Path(os.environ["STREAM_DATA_DIR"])
SAMPLE = PROJECT_DIR / "sample" / "recentchange_sample.jsonl"
LAKE_DIR = DATA_DIR / "lake"
BRONZE = LAKE_DIR / "bronze_events"
SILVER = LAKE_DIR / "silver_events"
DEAD_LETTER = LAKE_DIR / "dead_letter"
GOLD = LAKE_DIR / "gold_edits_per_minute"
CHECKPOINTS = DATA_DIR / "checkpoints"
KAFKA_BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP"]
KAFKA_TOPIC = os.environ["KAFKA_TOPIC"]
