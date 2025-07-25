import pandas as pd, logging
from typing import List, Tuple

def diff(old_df: pd.DataFrame, new_df: pd.DataFrame) -> Tuple[List[dict], List[dict], List[tuple]]:
    old_ids = set(old_df["ID"])
    new_ids = set(new_df["ID"])

    added_ids = new_ids - old_ids
    removed_ids = old_ids - new_ids
    common_ids = old_ids & new_ids

    added = [r.to_dict() for _, r in new_df[new_df["ID"].isin(added_ids)].iterrows()]
    removed = [r.to_dict() for _, r in old_df[old_df["ID"].isin(removed_ids)].iterrows()]

    old_dict = old_df.set_index('ID').to_dict(orient='index')
    new_dict = new_df.set_index('ID').to_dict(orient='index')

    changed = []
    for id_ in common_ids:
        old_row = old_dict.get(id_)
        # old_row = old_df[old_df["ID"] == id_].iloc[0]
        new_row = new_dict.get(id_)
        # new_row = new_df[new_df["ID"] == id_].iloc[0]
        cols = ["Остальные данные", "Доп Имя", "Террорист"]
        if any(str(old_row.get(c)) != str(new_row.get(c)) for c in cols):
            changed.append((old_row, new_row))
    logging.info("Added %d, Removed %d, Changed %d", len(added), len(removed), len(changed))
    return added, removed, changed