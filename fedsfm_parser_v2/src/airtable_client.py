import logging, time
from typing import List, Dict

import pandas as pd
import requests

from config import (
    AIRTABLE_API_KEY,
    AIRTABLE_BASE_ID,
    AIRTABLE_CACHE_DIR,
    AIRTABLE_FIELDS_MAIN,
)
from storage import _clean

API_BASE = "https://api.airtable.com/v0"


class AirtableClient:
    def __init__(self, api_key=AIRTABLE_API_KEY, base_id=AIRTABLE_BASE_ID):
        self.base_id = base_id
        self.headers = {"Authorization": f"Bearer {api_key}"}

    # ───────────────────────────────────────────────
    def _get(self, table: str, params: Dict):
        url = f"{API_BASE}/{self.base_id}/{table}"
        for attempt in range(1, 6):
            r = requests.get(url, headers=self.headers, timeout=60, params=params)
            logging.debug("GET %s (try %d) → %s", r.url, attempt, r.status_code)

            if r.status_code == 429:
                wait = int(r.headers.get("Retry-After", 2))
                logging.warning("429 Too Many Requests → sleep %ds", wait)
                time.sleep(wait)
                continue

            if r.status_code == 422:
                raise requests.HTTPError(
                    f"Airtable 422: проверьте название поля/VIEW. URL: {r.url}", response=r
                )
            r.raise_for_status()
            return r.json()
        raise RuntimeError("Airtable: превышено число попыток")

    # ───────────────────────────────────────────────
    def fetch_df(
        self,
        table: str,
        view: str | None = None,
        fields: List[str] | None = None,
        cache_name: str | None = None,
    ) -> pd.DataFrame:
        """
        Загружает записи Airtable, возвращая **только нужные колонки**.
        Без двойного URL-кодирования!
        """
        if fields is None:
            fields = AIRTABLE_FIELDS_MAIN
        params: Dict[str, list[str] | str] = {"fields[]": fields}
        if view:
            params["view"] = view

        # постраничная загрузка
        recs, page = [], 0
        while True:
            page += 1
            data = self._get(table, params)
            recs.extend(data["records"])
            logging.info("Airtable: page %d → %d rows total", page, len(recs))

            off = data.get("offset")
            if not off:
                break
            params["offset"] = off
            time.sleep(0.25)

        # DataFrame и фильтрация
        rows = [{**rec["fields"], "_air_id": rec["id"]} for rec in recs]
        df = pd.DataFrame(rows)
        df = df[[c for c in fields if c in df.columns] + ["_air_id"]]

        cache = AIRTABLE_CACHE_DIR / (cache_name or f"{table}.csv")
        df.to_csv(cache, index=False, encoding="utf-8")

        _clean(AIRTABLE_CACHE_DIR, "*.csv")
        
        logging.info("Airtable: итог %d строк, %d колонок", len(df), len(df.columns) - 1)
        return df, cache
