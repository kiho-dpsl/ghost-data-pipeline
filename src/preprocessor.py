from datetime import datetime, timedelta
import ast
import json

import pandas as pd

from config import DATA_DIR, KST


def extract_member_en_stats():
    members = pd.read_csv(f"{DATA_DIR}/members.csv")
    members= members[members['labels'].str.contains('english')]

    start_date = (datetime.now(tz=KST) - timedelta(days=7)).replace(hour=10, minute=0, second=0, microsecond=0)
    end_date = datetime.now(tz=KST).replace(hour=10, minute=0, second=0, microsecond=0)

    members['last_seen_at_kst'] = pd.to_datetime(members['last_seen_at']).dt.tz_convert('Asia/Seoul')

    date = datetime.now(tz=KST).date().strftime('%Y-%m-%d')
    total = len(members)
    active = len(members[(members['last_seen_at_kst'] >= start_date) & (members['last_seen_at_kst'] < end_date)])
    subscriber = len(members[members['subscribed'] == True])
    engagement_rate = round((active / total), 3)
    subscription_rate = round((subscriber / total), 3)

    stats = {
        'date': date,
        'total': total,
        'active': active,
        'subscriber': subscriber,
        'engagement_rate' : engagement_rate,
        'subscription_rate': subscription_rate
    }   

    with open(f"{DATA_DIR}/member_en_stats.json", 'w') as file:
        json.dump(stats, file, indent=4)


def extract_member_ko_stats():
    members = pd.read_csv(f"{DATA_DIR}/members.csv")
    members= members[members['labels'].str.contains('korean')]

    start_date = (datetime.now(tz=KST) - timedelta(days=7)).replace(hour=10, minute=0, second=0, microsecond=0)
    end_date = datetime.now(tz=KST).replace(hour=10, minute=0, second=0, microsecond=0)

    members['last_seen_at_kst'] = pd.to_datetime(members['last_seen_at']).dt.tz_convert('Asia/Seoul')

    date = datetime.now(tz=KST).date().strftime('%Y-%m-%d')
    total = len(members)
    active = len(members[(members['last_seen_at_kst'] >= start_date) & (members['last_seen_at_kst'] < end_date)])
    subscriber = len(members[members['subscribed'] == True])
    engagement_rate = round((active / total), 3)
    subscription_rate = round((subscriber / total), 3)

    stats = {
        'date': date,
        'total': total,
        'active': active,
        'subscriber': subscriber,
        'engagement_rate' : engagement_rate,
        'subscription_rate': subscription_rate
    }  

    with open(f"{DATA_DIR}/member_ko_stats.json", 'w') as file:
        json.dump(stats, file, indent=4)


def extract_subscriber_en_stats():
    members = pd.read_csv(f"{DATA_DIR}/members.csv")
    members= members[members['labels'].str.contains('english')]

    start_date = (datetime.now(tz=KST) - timedelta(days=7)).replace(hour=10, minute=0, second=0, microsecond=0)
    end_date = datetime.now(tz=KST).replace(hour=10, minute=0, second=0, microsecond=0)

    members['created_at_kst'] = pd.to_datetime(members['created_at']).dt.tz_convert('Asia/Seoul')
    members['last_seen_at_kst'] = pd.to_datetime(members['last_seen_at']).dt.tz_convert('Asia/Seoul')

    date = datetime.now(tz=KST).date().strftime('%Y-%m-%d')
    total = len(members[members['subscribed'] == True])
    new = len(members[(members['created_at_kst'] >= start_date) & (members['created_at_kst'] < end_date)])
    churn = len(members[(members['subscribed'] == False) & (members['last_seen_at_kst'] >= start_date) & (members['last_seen_at_kst'] < end_date)])
    net = new - churn

    stats = {
        'date': date,
        'total': total,
        'new': new,
        'churn': churn,
        'net': net
    }

    with open(f"{DATA_DIR}/subscriber_en_stats.json", 'w') as file:
        json.dump(stats, file, indent=4)


def extract_subscriber_ko_stats():
    members = pd.read_csv(f"{DATA_DIR}/members.csv")
    members= members[members['labels'].str.contains('korean')]

    start_date = (datetime.now(tz=KST) - timedelta(days=7)).replace(hour=10, minute=0, second=0, microsecond=0)
    end_date = datetime.now(tz=KST).replace(hour=10, minute=0, second=0, microsecond=0)

    members['created_at_kst'] = pd.to_datetime(members['created_at']).dt.tz_convert('Asia/Seoul')
    members['last_seen_at_kst'] = pd.to_datetime(members['last_seen_at']).dt.tz_convert('Asia/Seoul')

    date = datetime.now(tz=KST).date().strftime('%Y-%m-%d')
    total = len(members[members['subscribed'] == True])
    new = len(members[(members['created_at_kst'] >= start_date) & (members['created_at_kst'] < end_date)])
    churn = len(members[(members['subscribed'] == False) & (members['last_seen_at_kst'] >= start_date) & (members['last_seen_at_kst'] < end_date)])
    net = new - churn

    stats = {
        'date': date,
        'total': total,
        'new': new,
        'churn': churn,
        'net': net
    }

    with open(f"{DATA_DIR}/subscriber_ko_stats.json", 'w') as file:
        json.dump(stats, file, indent=4)


def extract_newsletter_en_stats():
    newsletters = pd.read_csv(f"{DATA_DIR}/newsletters.csv")
    newsletter = newsletters[newsletters['email_segment'].str.contains('english')].reset_index(drop=True)
    email_dict = ast.literal_eval(newsletter['email'].loc[0])
    count_dict = ast.literal_eval(newsletter['count'].loc[0])
    
    date = newsletter['published_date'].loc[0]
    sent = email_dict['email_count']
    opened = email_dict['opened_count']
    clicked = count_dict['clicks']
    delivered = email_dict['delivered_count']
    open_rate = round((opened / delivered), 3)
    click_rate = round((clicked / opened), 3)
    delivery_rate = round((delivered / sent), 3)
        
    stats = {
        'date': date,
        'sent': sent,
        'opened': opened,
        'clicked': clicked,
        'delivered': delivered,
        'open_rate': open_rate,
        'click_rate': click_rate,
        'delivery_rate': delivery_rate
    }

    with open(f"{DATA_DIR}/newsletter_en_stats.json", 'w') as file:
        json.dump(stats, file, indent=4)


def extract_newsletter_ko_stats():
    newsletters = pd.read_csv(f"{DATA_DIR}/newsletters.csv")
    newsletter = newsletters[newsletters['email_segment'].str.contains('korean')].reset_index(drop=True)
    email_dict = ast.literal_eval(newsletter['email'].loc[0])
    count_dict = ast.literal_eval(newsletter['count'].loc[0])
    
    date = newsletter['published_date'].loc[0]
    sent = email_dict['email_count']
    opened = email_dict['opened_count']
    clicked = count_dict['clicks']
    delivered = email_dict['delivered_count']
    open_rate = round((opened / delivered), 3)
    click_rate = round((clicked / opened), 3)
    delivery_rate = round((delivered / sent), 3)
        
    stats = {
        'date': date,
        'sent': sent,
        'opened': opened,
        'clicked': clicked,
        'delivered': delivered,
        'open_rate': open_rate,
        'click_rate': click_rate,
        'delivery_rate': delivery_rate
    }

    with open(f"{DATA_DIR}/newsletter_ko_stats.json", 'w') as file:
        json.dump(stats, file, indent=4)


def preprocess_data():
    extract_member_en_stats()
    extract_member_ko_stats()
    extract_subscriber_en_stats()
    extract_subscriber_ko_stats()
    extract_newsletter_en_stats()
    extract_newsletter_ko_stats()