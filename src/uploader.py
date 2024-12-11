from datetime import datetime
import json

import pandas as pd
import pandas_gbq

from config import (
    DATA_DIR,
    KST,
    PROJECT_ID,
    RAW_DATASET,
    STATS_DATASET,
    MEMBER_EN_STATS_TABLE,
    MEMBER_KO_STATS_TABLE,
    SUBSCRIBER_EN_STATS_TABLE,
    SUBSCRIBER_KO_STATS_TABLE,
    NEWSLETTER_EN_STATS_TABLE,
    NEWSLETTER_KO_STATS_TABLE,
    LOCATION
)
from utils import load_table


def upload_members():
    members = pd.read_csv(f"{DATA_DIR}/members.csv", dtype=str)
    date = datetime.now(tz=KST).strftime("%y%m%d")
    print('Uploading members...')
    pandas_gbq.to_gbq(members, f"{RAW_DATASET}.members_{date}", project_id=PROJECT_ID, if_exists='replace', location=LOCATION)


def upload_newsletters():
    table = load_table(RAW_DATASET, 'newsletters')
    newsletters = pd.read_csv(f"{DATA_DIR}/newsletters.csv", dtype=str)
    newsletters = newsletters[~newsletters['id'].isin(table['id'].values)]

    if newsletters.empty:
        return
    else:
        table = pd.concat([table, newsletters])
        print('Uploading newsletters...')
        pandas_gbq.to_gbq(table, f"{RAW_DATASET}.newsletters", project_id=PROJECT_ID, if_exists='replace', location=LOCATION)


def upload_member_en_stats():
    table = load_table(STATS_DATASET, MEMBER_EN_STATS_TABLE)

    with open(f"{DATA_DIR}/member_en_stats.json", 'r') as file:
        stats = json.load(file)

    date = stats['date']
    existing_row = table[table['date'] == date]

    if existing_row.empty:
        table.loc[len(table)] = list(stats.values())
    else:
        table.loc[existing_row.index] = list(stats.values())

    print("Uploading member_en_stats...")
    pandas_gbq.to_gbq(table, f"{STATS_DATASET}.{MEMBER_EN_STATS_TABLE}", project_id=PROJECT_ID, if_exists='replace')


def upload_member_ko_stats():
    table = load_table(STATS_DATASET, MEMBER_KO_STATS_TABLE)

    with open(f"{DATA_DIR}/member_ko_stats.json", 'r') as file:
        stats = json.load(file)

    date = stats['date']
    existing_row = table[table['date'] == date]

    if existing_row.empty:
        table.loc[len(table)] = list(stats.values())
    else:
        table.loc[existing_row.index] = list(stats.values())

    print("Uploading member_ko_stats...")
    pandas_gbq.to_gbq(table, f"{STATS_DATASET}.{MEMBER_KO_STATS_TABLE}", project_id=PROJECT_ID, if_exists='replace')


def upload_subscriber_en_stats():
    table = load_table(STATS_DATASET, SUBSCRIBER_EN_STATS_TABLE)

    with open(f"{DATA_DIR}/subscriber_en_stats.json", 'r') as file:
        stats = json.load(file)

    date = stats['date']
    existing_row = table[table['date'] == date]

    if existing_row.empty:
        table.loc[len(table)] = list(stats.values())
    else:
        table.loc[existing_row.index] = list(stats.values())

    print("Uploading subscriber_en_stats...")
    pandas_gbq.to_gbq(table, f"{STATS_DATASET}.{SUBSCRIBER_EN_STATS_TABLE}", project_id=PROJECT_ID, if_exists='replace')


def upload_subscriber_ko_stats():
    table = load_table(STATS_DATASET, SUBSCRIBER_KO_STATS_TABLE)

    with open(f"{DATA_DIR}/subscriber_ko_stats.json", 'r') as file:
        stats = json.load(file)

    date = stats['date']
    existing_row = table[table['date'] == date]

    if existing_row.empty:
        table.loc[len(table)] = list(stats.values())
    else:
        table.loc[existing_row.index] = list(stats.values())

    print("Uploading subscriber_ko_stats...")
    pandas_gbq.to_gbq(table, f"{STATS_DATASET}.{SUBSCRIBER_KO_STATS_TABLE}", project_id=PROJECT_ID, if_exists='replace')


def upload_newsletter_en_stats():
    table = load_table(STATS_DATASET, NEWSLETTER_EN_STATS_TABLE)

    with open(f"{DATA_DIR}/newsletter_en_stats.json", 'r') as file:
        stats = json.load(file)

    date = stats['date']
    existing_row = table[table['date'] == date]

    if existing_row.empty:
        table.loc[len(table)] = list(stats.values())
    else:
        table.loc[existing_row.index] = list(stats.values())
    
    print("Uploading newsletter_en_stats...")
    pandas_gbq.to_gbq(table, f"{STATS_DATASET}.{NEWSLETTER_EN_STATS_TABLE}", project_id=PROJECT_ID, if_exists='replace')


def upload_newsletter_ko_stats():
    table = load_table(STATS_DATASET, NEWSLETTER_KO_STATS_TABLE)

    with open(f"{DATA_DIR}/newsletter_ko_stats.json", 'r') as file:
        stats = json.load(file)

    date = stats['date']
    existing_row = table[table['date'] == date]

    if existing_row.empty:
        table.loc[len(table)] = list(stats.values())
    else:
        table.loc[existing_row.index] = list(stats.values())
    
    print("Uploading newsletter_ko_stats...")
    pandas_gbq.to_gbq(table, f"{STATS_DATASET}.{NEWSLETTER_KO_STATS_TABLE}", project_id=PROJECT_ID, if_exists='replace')
   

def upload_data():
    upload_members()
    upload_newsletters()
    upload_member_en_stats()
    upload_member_ko_stats()
    upload_subscriber_en_stats()
    upload_subscriber_ko_stats()
    upload_newsletter_en_stats()
    upload_newsletter_ko_stats()