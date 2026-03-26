import pandas as pd
import ast
from fastapi import FastAPI
from datetime import datetime, timedelta


app = FastAPI()
from fastapi.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # later restrict this
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
@app.get("/class")
def get_class(
    min_val: int = 1000,
    days: int = 30,
    player_class: str = "Mage",
    KP: str = "GKP"):
    url2 = "https://docs.google.com/spreadsheets/d/1Izu2wSmi0aEQCWTvfLAXX0ucXR2223ILzFxTiQXcl80/gviz/tq?tqx=out:csv&sheet={KP}".format(KP=KP)
    url3 = "https://docs.google.com/spreadsheets/d/1Izu2wSmi0aEQCWTvfLAXX0ucXR2223ILzFxTiQXcl80/gviz/tq?tqx=out:csv&sheet=Roster"

    last = "Last Raid"
    dfRoster = pd.read_csv(url3)

    # Get class names
    class_target = player_class.lower()
    class_target = class_target.capitalize()
    class_specific = dfRoster.loc[dfRoster["Class"] == class_target, "Name"]

    dfkp = pd.read_csv(url2)
    



    # Clean + filter in one pipeline
    dfkp_class = (
        dfkp
        .assign(**{
            last: pd.to_datetime(dfkp[last], format="mixed", dayfirst=True, errors="coerce")
        })
        .dropna(subset=[last])
        .loc[
            lambda d: d["Player Name"].isin(class_specific)
            & (d["Current"] > min_val)
            & (d[last] >= datetime.now() - timedelta(days=days))
        ]
        .sort_values("Current", ascending=False)
        [["Player Name", "Current", last]]
    )

    return dfkp_class.to_dict(orient="records")

@app.get("/individual")
def get_individual(
    character: str = "FrozenRage",
    days: int = 30
):

    character = character.lower()
    url = "https://docs.google.com/spreadsheets/d/1Izu2wSmi0aEQCWTvfLAXX0ucXR2223ILzFxTiQXcl80/gviz/tq?tqx=out:csv&sheet=Bosses"
    df = pd.read_csv(url)



    df["Datetime"] = pd.to_datetime(df["Datetime"], dayfirst=True)

    df["Attendees"] = df["Attendees"].apply(ast.literal_eval).apply(lambda x: [attendee.lower() for attendee in x])


    # Filter last 30 days
    cutoff = datetime.now() - timedelta(days=days)
    df_recent = df[df["Datetime"] >= cutoff]

    df_rbpp = df_recent[df_recent["KP Pool"] == "RBPP"]

    # ---- COUNT 1: total RBPP bosses ----
    total_rbpp = len(df_rbpp)

    # ---- COUNT 2: RBPP bosses with specific attendee ----

    count_with_attendee = df_rbpp[
        df_rbpp["Attendees"].apply(lambda x: character in x)
    ].shape[0]

    percentage = round(count_with_attendee / total_rbpp * 100, 2) if total_rbpp > 0 else 0
    return {
        "character": character,
        "total_rbpp_bosses": total_rbpp,
        "count_with_attendee": count_with_attendee,
        "percentage": percentage
    }
