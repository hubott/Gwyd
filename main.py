import pandas as pd
import ast
from fastapi import FastAPI, Query
from datetime import datetime, timedelta
from typing import List
pd.set_option('display.max_rows', None)      # show all rows
pd.set_option('display.max_columns', None)  # show all columns
pd.set_option('display.width', None)        # don't wrap lines


pd.set_option('display.max_colwidth', None)


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
    player_classes: List[str] = Query(default=["Mage"]),
    KP: str = "GKP"):
    url2 = "https://docs.google.com/spreadsheets/d/1Izu2wSmi0aEQCWTvfLAXX0ucXR2223ILzFxTiQXcl80/gviz/tq?tqx=out:csv&sheet={KP}".format(KP=KP)
    url3 = "https://docs.google.com/spreadsheets/d/1Izu2wSmi0aEQCWTvfLAXX0ucXR2223ILzFxTiQXcl80/gviz/tq?tqx=out:csv&sheet=Roster"

    last = "Last Raid"
    dfRoster = pd.read_csv(url3)

    results = []

    for cls in player_classes:
        cls_clean = cls.lower().capitalize()  # normalize class name
        class_specific = dfRoster.loc[dfRoster["Class"] == cls_clean, "Name"]

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

        results.append({
            "class": cls_clean,
            "players": dfkp_class.to_dict(orient="records")
        })

    return results

@app.get("/individual")
def get_individual(
    character: str = "FrozenRage",
    days: int = 30,
    KP: str = "RBPP"
):

    character = character.lower()
    url = "https://docs.google.com/spreadsheets/d/1Izu2wSmi0aEQCWTvfLAXX0ucXR2223ILzFxTiQXcl80/gviz/tq?tqx=out:csv&sheet=Bosses"
    df = pd.read_csv(url)



    df["Datetime"] = pd.to_datetime(df["Datetime"], dayfirst=True)

    df["Attendees"] = df["Attendees"].apply(ast.literal_eval).apply(lambda x: [attendee.lower() for attendee in x])


    # Filter last 30 days
    cutoff = datetime.now() - timedelta(days=days)
    df_recent = df[df["Datetime"] >= cutoff]

    df_rbpp = df_recent[df_recent["KP Pool"] == KP]

    # ---- COUNT 1: total RBPP bosses ----
    total_rbpp = len(df_rbpp)

    # ---- COUNT 2: RBPP bosses with specific attendee ----

    count_with_attendee = df_rbpp[
        df_rbpp["Attendees"].apply(lambda x: character in x)
    ].shape[0]

    percentage = round(count_with_attendee / total_rbpp * 100, 2) if total_rbpp > 0 else 0
    return {
        "Character": character,
        f"Total Bosses in last {days} days": total_rbpp,
        f"Number attended by {character}": count_with_attendee,
        "Percentage": percentage
    }



@app.get("/class_attendance")
def get_class_attendance(
    min_val: int = 0,
    days: int = 30,
    player_classes: List[str] = Query(default=["Mage"]),
    KP: str = "GKP"):
    roster_url = "https://docs.google.com/spreadsheets/d/1Izu2wSmi0aEQCWTvfLAXX0ucXR2223ILzFxTiQXcl80/gviz/tq?tqx=out:csv&sheet=Roster"
    bosses_url = "https://docs.google.com/spreadsheets/d/1Izu2wSmi0aEQCWTvfLAXX0ucXR2223ILzFxTiQXcl80/gviz/tq?tqx=out:csv&sheet=Bosses"

    last = "Last Raid"
    dfRoster = pd.read_csv(roster_url)
    dfBosses = pd.read_csv(bosses_url)

    dfBosses["Datetime"] = pd.to_datetime(dfBosses["Datetime"], dayfirst=True, errors="coerce")

    def ensure_list(x):
        if isinstance(x, str):
            return ast.literal_eval(x)
        return x

    dfBosses["Attendees"] = dfBosses["Attendees"].apply(ensure_list).apply(
        lambda x: [attendee.lower() for attendee in x] if isinstance(x, list) else []
    )

    cutoff = datetime.now() - timedelta(days=days)
    df_recent = dfBosses[dfBosses["Datetime"] >= cutoff]
    if KP:
        df_recent = df_recent[df_recent["KP Pool"] == KP]

    results = []

    for cls in player_classes:
        cls_clean = cls.lower().capitalize()  # normalize class name
        class_specific = dfRoster.loc[dfRoster["Class"] == cls_clean, "Name"].dropna().astype(str)

        players = []
        for player in class_specific:
            player_lower = player.lower()
            player_runs = df_recent[df_recent["Attendees"].apply(lambda attendees: player_lower in attendees)]
            bosses_attended = int(player_runs.shape[0])
            if bosses_attended >= min_val:
                last_raid_date = player_runs["Datetime"].max()
                players.append({
                    "Player Name": player,
                    "Bosses Attended": bosses_attended,
                    last: last_raid_date if pd.notna(last_raid_date) else None,
                })

        results.append({
            "class": cls_clean,
            "players": sorted(players, key=lambda p: p["Bosses Attended"], reverse=True)
        })

    return results



char1 = "cathbad"
char2 = "Beryl"
url = "https://docs.google.com/spreadsheets/d/1Izu2wSmi0aEQCWTvfLAXX0ucXR2223ILzFxTiQXcl80/gviz/tq?tqx=out:csv&sheet=Bosses"
urll = "https://docs.google.com/spreadsheets/d/1Izu2wSmi0aEQCWTvfLAXX0ucXR2223ILzFxTiQXcl80/gviz/tq?tqx=out:csv&sheet=Roster"
df = pd.read_csv(url)
df = df[df["KP Pool"] == "RBPP"]  # filter for RBPP only
dfRoster = pd.read_csv(urll)
"""
nick = []
for index, row in dfRoster.iterrows():
    if str(row["Main Character"]).lower() == "nick18":
        nick.append(row["Name"])
print(nick)
"""
df["Datetime"] = pd.to_datetime(df["Datetime"], dayfirst=True)
#df = df[df["Boss"] != "rd"]
#df = df[df["Boss"] != "Tree"]

# --- If Attendees is stored as a string, convert to list ---
def ensure_list(x):
    if isinstance(x, str):
        return ast.literal_eval(x)
    return x

df["Attendees"] = df["Attendees"].apply(ensure_list)

# --- Filter last 90 days ---
cutoff_date = datetime.now() - timedelta(days=365)
df_recent = df[df["Datetime"] >= cutoff_date]

# --- Condition: Dianic present AND Nick18 absent ---
def condition(attendees):
    return (char1 in attendees) and not (char2 in attendees)
def condition2(attendees):
    return (char2 in attendees) and not (char1 in attendees)

nicktotal = df_recent["Attendees"].apply(lambda x: char1 in x).sum()
diantotal = df_recent["Attendees"].apply(lambda x: char2 in x).sum()



nnd = df_recent["Attendees"].apply(condition).sum()
dnn = df_recent["Attendees"].apply(condition2).sum()

filtered_runs = df_recent[df_recent["Attendees"].apply(condition)]
#print(filtered_runs)

print(f"Number of runs where {char1} attended without {char2}: {nnd}")
print(f"Number of runs where {char2} attended without {char1}: {dnn}")
print(f"Total runs: {char1}: {nicktotal}, {char2}: {diantotal}")
print(f"Percentage of {char1} runs with {char1} but not {char2}: {round(nnd/nicktotal*100, 2) if nicktotal > 0 else 0}%")
print(f"Percentage of runs {char2} with {char2} but not {char1}: {round(dnn/diantotal*100, 2) if diantotal > 0 else 0}%")