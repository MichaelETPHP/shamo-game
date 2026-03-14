"""
Check presence of qr_codes and qr_scans in the configured database and report search_path.
Run: python scripts/check_qr_tables.py
"""
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / '.env')
DATABASE_URL = os.getenv('DATABASE_URL')
DB_SCHEMA = os.getenv('DB_SCHEMA') or 'public'

if not DATABASE_URL:
    print('DATABASE_URL not set in .env')
    raise SystemExit(2)

try:
    import psycopg2
except Exception as e:
    print('psycopg2 not available:', e)
    raise SystemExit(3)

print('Connecting to:', DATABASE_URL.split('@')[-1])
conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()

# show current search_path for this session
cur.execute("SHOW search_path;")
print('search_path =', cur.fetchone()[0])

# check for shamo.qr_codes
cur.execute("SELECT table_schema, table_name FROM information_schema.tables WHERE table_name IN ('qr_codes','qr_scans') ORDER BY table_schema, table_name;")
rows = cur.fetchall()
if not rows:
    print('No qr_codes or qr_scans tables found in information_schema for this DB.')
else:
    for s,t in rows:
        print('found table:', s + '.' + t)

# try an explicit schema-qualified select if exists
try:
    cur.execute("SELECT count(*) FROM shamo.qr_codes;")
    print('shamo.qr_codes count =', cur.fetchone()[0])
except Exception as e:
    print('shamo.qr_codes select error:', repr(e))

try:
    cur.execute("SELECT count(*) FROM shamo.qr_scans;")
    print('shamo.qr_scans count =', cur.fetchone()[0])
except Exception as e:
    print('shamo.qr_scans select error:', repr(e))

cur.close()
conn.close()
print('Done.')
