"""
Simple smoke tests for core API endpoints used by admin UI and game.
Run: python scripts/api_smoke_test.py
Reads API_BASE_URL and ADMIN_TOKEN from .env (falls back to localhost).
"""
import os
from pathlib import Path
from dotenv import load_dotenv
import httpx

load_dotenv(Path(__file__).resolve().parents[1] / '.env')
API_BASE = os.getenv('API_BASE_URL') or 'http://localhost:8001'
API = API_BASE.rstrip('/') + '/shamo/api'
ADMIN_TOKEN = os.getenv('ADMIN_TOKEN') or 'shamo_admin_2024'

ENDPOINTS = [
    ('GET', '/qr'),
    ('GET', '/qr/scans?limit=5'),
    ('GET', '/games?page=1&per_page=5'),
    ('GET', '/companies?page=1&per_page=5&status=active'),
    ('GET', '/public/games/active'),
    ('GET', '/public/games/debug'),
    ('GET', '/public/game-config'),
]

headers = {'X-Admin-Token': ADMIN_TOKEN, 'Content-Type': 'application/json'}

print('API base:', API)

with httpx.Client(timeout=10) as client:
    for method, path in ENDPOINTS:
        url = API + path if path.startswith('/') else API + '/' + path
        try:
            print('\n->', method, url)
            if method == 'GET':
                res = client.get(url, headers=headers)
            else:
                res = client.request(method, url, headers=headers)
            print('Status:', res.status_code)
            try:
                print('JSON:', res.json())
            except Exception:
                print('Text:', res.text[:400])
        except Exception as e:
            print('ERROR:', e)

# Test a POST create QR flow requires admin auth and body; we try a dry run to /api/qr with minimal data
print('\n-> POST (create QR) with minimal payload')
qr_payload = {
    'game_id': None,
    'company_id': None,
    'label': 'SmokeTest QR',
    'max_scans': 0,
    'expiry_hours': 0,
    'base_url': 'https://example.com/game'
}
try:
    with httpx.Client(timeout=10) as client:
        res = client.post(API + '/qr', headers=headers, json=qr_payload)
        print('Status:', res.status_code)
        try: print('JSON:', res.json())
        except: print('Text:', res.text[:400])
except Exception as e:
    print('ERROR:', e)
