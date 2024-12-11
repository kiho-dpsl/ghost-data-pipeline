from google.cloud import bigquery

from config import (
    PROJECT_ID,
    STATS_DATASET,
    MEMBER_EN_STATS_TABLE,
    MEMBER_KO_STATS_TABLE,
    SUBSCRIBER_EN_STATS_TABLE,
    SUBSCRIBER_KO_STATS_TABLE,
    NEWSLETTER_EN_STATS_TABLE,
    NEWSLETTER_KO_STATS_TABLE,
    LOCATION
)


def create_member_en_stats():
    client = bigquery.Client(project=PROJECT_ID)
    schema = [
        bigquery.SchemaField('date', 'STRING'),
        bigquery.SchemaField('total', 'INTEGER'),
        bigquery.SchemaField('active', 'INTEGER'),
        bigquery.SchemaField('subscriber', 'INTEGER'),
        bigquery.SchemaField('engagement_rate', 'FLOAT'),
        bigquery.SchemaField('subscription_rate', 'FLOAT')
    ]
    subscriber_en_stats = bigquery.Table(f"{PROJECT_ID}.{STATS_DATASET}.{MEMBER_EN_STATS_TABLE}", schema=schema)
    client.create_table(subscriber_en_stats, exists_ok=True, location=LOCATION)


def create_member_ko_stats():
    client = bigquery.Client(project=PROJECT_ID)
    schema = [
        bigquery.SchemaField('date', 'STRING'),
        bigquery.SchemaField('total', 'INTEGER'),
        bigquery.SchemaField('active', 'INTEGER'),
        bigquery.SchemaField('subscriber', 'INTEGER'),
        bigquery.SchemaField('engagement_rate', 'FLOAT'),
        bigquery.SchemaField('subscription_rate', 'FLOAT')
    ]
    subscriber_ko_stats = bigquery.Table(f"{PROJECT_ID}.{STATS_DATASET}.{MEMBER_KO_STATS_TABLE}", schema=schema)
    client.create_table(subscriber_ko_stats, exists_ok=True, location=LOCATION)


def create_subscriber_en_stats():
    client = bigquery.Client(project=PROJECT_ID)
    schema = [
        bigquery.SchemaField('date', 'STRING'),
        bigquery.SchemaField('total', 'INTEGER'),
        bigquery.SchemaField('new', 'INTEGER'),
        bigquery.SchemaField('churn', 'INTEGER'),
        bigquery.SchemaField('net', 'INTEGER')
    ]
    subscriber_en_stats = bigquery.Table(f"{PROJECT_ID}.{STATS_DATASET}.{SUBSCRIBER_EN_STATS_TABLE}", schema=schema)
    client.create_table(subscriber_en_stats, exists_ok=True, location=LOCATION)


def create_subscriber_ko_stats():
    client = bigquery.Client(project=PROJECT_ID)
    schema = [
        bigquery.SchemaField('date', 'STRING'),
        bigquery.SchemaField('total', 'INTEGER'),
        bigquery.SchemaField('new', 'INTEGER'),
        bigquery.SchemaField('churn', 'INTEGER'),
        bigquery.SchemaField('net', 'INTEGER')
    ]
    subscriber_ko_stats = bigquery.Table(f"{PROJECT_ID}.{STATS_DATASET}.{SUBSCRIBER_KO_STATS_TABLE}", schema=schema)
    client.create_table(subscriber_ko_stats, exists_ok=True, location=LOCATION)


def create_newsletter_en_stats():
    client = bigquery.Client(project=PROJECT_ID)
    schema = [
        bigquery.SchemaField('date', 'STRING'),
        bigquery.SchemaField('sent', 'INTEGER'),
        bigquery.SchemaField('opened', 'INTEGER'),
        bigquery.SchemaField('clicked', 'INTEGER'),
        bigquery.SchemaField('delivered', 'STRING'),
        bigquery.SchemaField('open_rate', 'FLOAT'),
        bigquery.SchemaField('click_rate', 'FLOAT'),
        bigquery.SchemaField('delivery_rate', 'FLOAT')
    ]
    newsletter_en_stats = bigquery.Table(f"{PROJECT_ID}.{STATS_DATASET}.{NEWSLETTER_EN_STATS_TABLE}", schema=schema)
    client.create_table(newsletter_en_stats, exists_ok=True, location=LOCATION)


def create_newsletter_ko_stats():
    client = bigquery.Client(project=PROJECT_ID)
    schema = [
        bigquery.SchemaField('date', 'STRING'),
        bigquery.SchemaField('sent', 'INTEGER'),
        bigquery.SchemaField('opened', 'INTEGER'),
        bigquery.SchemaField('clicked', 'INTEGER'),
        bigquery.SchemaField('delivered', 'STRING'),
        bigquery.SchemaField('open_rate', 'FLOAT'),
        bigquery.SchemaField('click_rate', 'FLOAT'),
        bigquery.SchemaField('delivery_rate', 'FLOAT')
    ]
    newsletter_ko_stats = bigquery.Table(f"{PROJECT_ID}.{STATS_DATASET}.{NEWSLETTER_KO_STATS_TABLE}", schema=schema)
    client.create_table(newsletter_ko_stats, exists_ok=True, location=LOCATION)


def create_table():
    create_member_en_stats()
    create_member_ko_stats()
    create_subscriber_en_stats()
    create_subscriber_ko_stats()
    create_newsletter_en_stats()
    create_newsletter_ko_stats()