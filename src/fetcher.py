from datetime import datetime, timedelta

import requests
import pandas as pd
import pytz

from auth.ghost.token_generator import generate_token
from config import GHOST_ADMIN_URL, DATA_DIR, KST


def fetch_members():
    token = generate_token()
    url = f"{GHOST_ADMIN_URL}/members/"
    headers = {'Authorization': f'Ghost {token}'}

    page = 1
    members = []
    while True:
        r = requests.get(f"{url}?limit=all&page={page}", headers=headers)
        data = r.json()
        member = data['members']
        if not member:
            break
        members.extend(member)
        page += 1

    members = pd.DataFrame(members)
    members.to_csv(f"{DATA_DIR}/members.csv", index=False)


def fetch_newsletters():
    token = generate_token()
    url = f"{GHOST_ADMIN_URL}/posts/"
    headers = {'Authorization': f'Ghost {token}'}
    r = requests.get(url, headers=headers)
    data = r.json()
    posts = pd.DataFrame(data['posts'])
    
    newsletters = posts[posts['tags'].apply(lambda tags: any(tag['slug'] == 'newsletters' for tag in tags))]
    newsletters = newsletters[newsletters['newsletter'].notnull()]

    start_date = (datetime.now(tz=KST) - timedelta(days=7)).date()
    end_date = datetime.now(tz=KST).date()
    
    newsletters['published_date'] = pd.to_datetime(newsletters['published_at']).dt.tz_convert('Asia/Seoul').dt.date
    newsletters = newsletters[(newsletters['published_date'] >= start_date) & (newsletters['published_date'] < end_date)]
    newsletters.to_csv(f"{DATA_DIR}/newsletters.csv", index=False)


def fetch_data():
    fetch_members()
    fetch_newsletters()