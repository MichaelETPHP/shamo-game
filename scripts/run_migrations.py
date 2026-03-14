"""
Run SHAMO SQL migrations from migrations/shamo-schema-full.sql using DATABASE_URL.
Usage: python scripts/run_migrations.py
Requires: psycopg2 installed in the active environment.
"""
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / '.env')
DATABASE_URL = os.getenv('DATABASE_URL')
SQL_PATH = Path(__file__).resolve().parents[1] / 'migrations' / 'shamo-schema-full.sql'

if not DATABASE_URL:
    print('ERROR: DATABASE_URL not set in .env')
    sys.exit(2)
if not SQL_PATH.exists():
    print('ERROR: migrations file not found:', SQL_PATH)
    sys.exit(3)

try:
    import psycopg2
except Exception as e:
    print('ERROR: psycopg2 not installed in this environment:', e)
    sys.exit(4)

print('Connecting to DB...')
conn = psycopg2.connect(DATABASE_URL)
conn.autocommit = True
cur = conn.cursor()

print('Ensuring schema "shamo" exists...')
cur.execute("CREATE SCHEMA IF NOT EXISTS shamo;")

print('Executing migration SQL (this may take a while)...')
sql = SQL_PATH.read_text(encoding='utf-8')
try:
    cur.execute(sql)
    print('Migrations executed successfully.')
except Exception as e:
    print('ERROR executing migrations:', e)
    sys.exit(5)
finally:
    cur.close()
    conn.close()

print('Done.')
