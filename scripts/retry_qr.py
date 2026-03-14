import httpx
paths = ['/shamo/api/qr','/shamo/api/qr/scans?limit=5']
for p in paths:
    url = 'http://localhost:8001' + p
    try:
        print('\nREQUEST ->', url)
        r = httpx.get(url, headers={'X-Admin-Token':'shamo_admin_2024'}, timeout=60.0)
        print('STATUS', r.status_code)
        try:
            print('JSON:', r.json())
        except Exception:
            print('TEXT:', r.text[:1000])
    except Exception as e:
        print('EXCEPTION:', repr(e))
