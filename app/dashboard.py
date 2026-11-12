import pandas as pd
import streamlit as st
from deltalake import DeltaTable

from stream import paths

st.set_page_config(page_title="Wikipedia edits, live", layout="wide")
st.title("Wikipedia edits, live")


@st.fragment(run_every=10)
def board():
    path = paths.LAKE_DIR / "gold_edits_per_minute"
    if not path.exists():
        st.info("No finished minute yet. Start the producer and the job.")
        return
    df = DeltaTable(str(path)).to_pandas()
    df["window_start"] = pd.to_datetime(df["window_start"], utc=True)
    recent = df[df["window_start"] >= df["window_start"].max() - pd.Timedelta(minutes=15)]
    per_wiki = recent.groupby("wiki")[["edits", "bot_edits"]].sum().sort_values("edits", ascending=False)
    per_wiki["bot_share"] = (per_wiki["bot_edits"] / per_wiki["edits"]).round(2)
    a, b = st.columns(2)
    a.subheader("Edits per minute, all wikis")
    a.line_chart(df.groupby("window_start")["edits"].sum())
    b.subheader("Busiest wikis, last 15 minutes")
    b.dataframe(per_wiki.head(15))


board()
