import os
import requests
import pandas as pd
import logging
import time

from datetime import datetime

from consts import AIRTABLE_TOKEN, AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME, AIRTABLE_FOLDER
from main import setup_logger

def read_airtable_to_dataframe(api_key: str, base_id: str, table_name: str, data_filename: str) -> pd.DataFrame:
    """
    Fetches all records from an Airtable table and returns them as a pandas DataFrame.

    Args:
        api_key (str): Airtable API key (Bearer token).
        base_id (str): The Airtable base ID (e.g. 'appXXXXXXXXXXXXXX').
        table_name (str): The name of the table.

    Returns:
        pd.DataFrame: Table data as DataFrame.
    """
    headers = {
        "Authorization": f"Bearer {api_key}"
    }

    airtable_filepath = os.path.join(AIRTABLE_FOLDER, data_filename)

    url = f"https://api.airtable.com/v0/{base_id}/{table_name}"
    records = []
    offset = None

    while True:
        params = {}

        if offset:
            params['offset'] = offset

        logging.debug(f"Fetching records from {url} with params {params}")

        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()

        logging.debug(f"Received {len(data['records'])} records (offset: {offset}, records: {len(records)})")

        # logging.debug(data)

        for record in data['records']:
            fields = record.get('fields', {})
            fields['id'] = record['id']  # include Airtable's record ID
            records.append(fields)

        offset = data.get('offset')
        if not offset:
            break

        time.sleep(0.25)  # 4 requests/sec, under 5/sec limit

    df = pd.DataFrame(records)

    df.to_csv(airtable_filepath, index=False, encoding='utf-8')
    logging.info(f"Данные сохранены в {airtable_filepath}")

    return df


if __name__ == "__main__":
    setup_logger()

    data_filename = datetime.now().strftime("airtable_%Y_%m_%d__%H_%M_%S.csv")

    df = read_airtable_to_dataframe(AIRTABLE_TOKEN, AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME, data_filename)
    print(df.head())
