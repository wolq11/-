import sqlite3
conn = sqlite3.connect('data/club_stats.db')
conn.row_factory = sqlite3.Row

# Check plan_text and summary_text content
rows = conn.execute("SELECT id, plan_text, summary_text FROM checkin_sessions WHERE plan_text != '' OR summary_text != '' LIMIT 5").fetchall()
print('=== checkin_sessions plan_text / summary_text ===')
for r in rows:
    pt = r['plan_text'][:100] if r['plan_text'] else '(empty)'
    st = r['summary_text'][:100] if r['summary_text'] else '(empty)'
    print(f'  id={r["id"]}: plan_text={pt}')
    print(f'  id={r["id"]}: summary_text={st}')

# Check club_profiles description
rows2 = conn.execute("SELECT club_name, description FROM club_profiles WHERE description != '' LIMIT 3").fetchall()
print('\n=== club_profiles description ===')
for r in rows2:
    print(f'  {r["club_name"]}: {r["description"][:100]}')

# Check recruitment descriptions
rows3 = conn.execute("SELECT title, description FROM recruitments WHERE description != '' LIMIT 3").fetchall()
print('\n=== recruitments description ===')
for r in rows3:
    print(f'  {r["title"]}: {r["description"][:100]}')

conn.close()
