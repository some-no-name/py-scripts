import pandas as pd, logging
from datetime import datetime
from pathlib import Path
from config import DATA_DIR, REPORTS_DIR, SRC_DIR, KEEP_FILES_COUNT, MASTER_FILE

def get_region_dict():
    df = pd.read_csv(f"{SRC_DIR}/regions_dict.csv", dtype=str, sep=",")

    # Convert to the desired dictionary structure
    region_dict = {
        row["id"]: (
            row["Регион (справочник)"],
            [variant.strip() for variant in row["Варианты в файле"].split(";") if variant.strip()]
        )
        for _, row in df.iterrows()
    }

    return region_dict

def save_dataset(df: pd.DataFrame, ts: datetime) -> Path:
    DATA_DIR.mkdir(exist_ok=True, parents=True)
    path = DATA_DIR / f"data_{ts:%Y_%m_%d__%H_%M_%S}.csv"
    # comment for debug
    df.to_csv(path, index=False, encoding='utf-8')
    return path

def save_report(data: str, ts: datetime) -> Path:
    REPORTS_DIR.mkdir(exist_ok=True, parents=True)
    path = REPORTS_DIR / f"report_{ts:%Y_%m_%d__%H_%M_%S}.txt"
    with open(path, "w", encoding="utf-8") as f:
        f.write(data)
    return path

def load_latest(exclude: str = None):
# def load_latest(exclude: Path | None = None) -> pd.DataFrame | None:
    files = sorted(DATA_DIR.glob("data_*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
    for f in files:
        if exclude and f == exclude:
            continue
        try:
            return pd.read_csv(f, dtype=str), f
        except Exception as e:
            logging.warning("Cannot read %s: %s", f, e)
    return None, None

def update_master(new_df: pd.DataFrame, ts: datetime):
    if MASTER_FILE.exists():
        master = pd.read_csv(MASTER_FILE, dtype=str)
    else:
        master = pd.DataFrame(columns=list(new_df.columns) + ["Дата добавления","Дата удаления"])
        

    master_ids = set(master["ID"])
    new_ids = set(new_df["ID"])

    added = new_ids - master_ids
    if added:
        rows = new_df[new_df["ID"].isin(added)].copy()
        rows["Дата добавления"] = ts.strftime("%Y-%m-%d")
        rows["Дата удаления"] = ""
        master = pd.concat([master, rows], ignore_index=True)

    removed = master_ids - new_ids
    if removed:
        mask = master["ID"].isin(removed) & (master["Дата удаления"] == "")
        master.loc[mask, "Дата удаления"] = ts.strftime("%Y-%m-%d")

    master.to_csv(MASTER_FILE, index=False, encoding='utf-8')
    logging.info("Master table updated: %s", MASTER_FILE)
