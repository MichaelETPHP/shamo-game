import psycopg2, json
from psycopg2.extras import RealDictCursor

conn=psycopg2.connect('postgresql://postgres:XIwVLfIkIDIRpbdSd5RKosU88hiaeIjF@187.77.12.130:5433/shamo_db',options='-c search_path=public')
cur=conn.cursor(cursor_factory=RealDictCursor)

# Tables
cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public' ORDER BY table_name")
tables=[r['table_name'] for r in cur.fetchall()]

result={"tables":tables,"columns":{}}
for tbl in tables:
    cur.execute(f"SELECT column_name,data_type FROM information_schema.columns WHERE table_schema='public' AND table_name='{tbl}' ORDER BY ordinal_position")
    result["columns"][tbl]=[r['column_name'] for r in cur.fetchall()]

conn.close()
with open("tmp_schema_out.json","w") as f:
    json.dump(result,f,indent=2)
print("Done")
