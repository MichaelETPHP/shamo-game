"""
Apply a single SQL migration file (path relative to repo root).
Usage: python scripts/apply_sql_file.py migrations/021_qr_scans.sql
"""
import sys
import os
from pathlib import Path
from dotenv import load_dotenv

if len(sys.argv) < 2:
    print('Usage: python scripts/apply_sql_file.py <sql-file>')
    sys.exit(2)

sql_file = Path(sys.argv[1])
if not sql_file.exists():
    print('SQL file not found:', sql_file)
    sys.exit(3)

load_dotenv(Path(__file__).resolve().parents[1] / '.env')
DATABASE_URL = os.getenv('DATABASE_URL')
if not DATABASE_URL:
    print('DATABASE_URL not set in .env')
    sys.exit(4)

try:
    import psycopg2
except Exception as e:
    print('psycopg2 not installed:', e)
    sys.exit(5)

print('Applying', sql_file)
conn = psycopg2.connect(DATABASE_URL)
conn.autocommit = True
cur = conn.cursor()
try:
    sql = sql_file.read_text(encoding='utf-8')
    cur.execute(sql)
    print('Applied successfully')
except Exception as e:
    print('ERROR applying SQL:', e)
    sys.exit(6)
finally:
    cur.close()
    conn.close()
