import os
from pathlib import Path

PROJECT_DIR = Path(os.environ["STREAM_PROJECT_DIR"])
DATA_DIR = Path(os.environ["STREAM_DATA_DIR"])
SAMPLE = PROJECT_DIR / "sample" / "recentchange_sample.jsonl"
LAKE_DIR = DATA_DIR / "lake"
CHECKPOINT_DIR = DATA_DIR / "checkpoints"
KAFKA_BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP"]
KAFKA_TOPIC = os.environ["KAFKA_TOPIC"]
