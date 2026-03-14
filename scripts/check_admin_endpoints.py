from dotenv import load_dotenv
from pathlib import Path
import os
import httpx

load_dotenv(Path(__file__).resolve().parents[1] / '.env')
API = (os.getenv('API_BASE_URL') or 'http://localhost:8001').rstrip('/') + '/shamo/api'
ADMIN_TOKEN = os.getenv('ADMIN_TOKEN') or 'shamo_admin_2024'

headers = {'X-Admin-Token': ADMIN_TOKEN}

with httpx.Client(timeout=20.0) as client:
    print('API base:', API)
    r = client.get(API + '/companies?per_page=5', headers=headers)
    print('GET /companies', r.status_code)
    try:
        comps = r.json()
    except Exception:
        print('BODY:', r.text[:1000]); comps = {}
    print('companies total =', comps.get('total'))
    if comps.get('data'):
        first = comps['data'][0]
        cid = first.get('id')
        print('\nFetching company', cid)
        rc = client.get(API + f'/companies/{cid}', headers=headers)
        print('GET /companies/{cid}', rc.status_code)
        try:
            print(rc.json())
        except Exception:
            print(rc.text[:1000])

    rg = client.get(API + '/games?page=1&per_page=5', headers=headers)
    print('\nGET /games', rg.status_code)
    try:
        games = rg.json()
    except Exception:
        print('GAMES BODY:', rg.text[:1000]); games = {}
    if games.get('data'):
        gid = games['data'][0].get('id')
        print('\nFetching game questions for', gid)
        rq = client.get(API + f'/games/{gid}/questions', headers=headers)
        print('GET /games/{gid}/questions', rq.status_code)
        try:
            print(rq.json())
        except Exception:
            print(rq.text[:1000])
    else:
        print('no games to inspect')
