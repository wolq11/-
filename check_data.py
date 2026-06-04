import sqlite3
conn = sqlite3.connect('data/club_stats.db')
conn.row_factory = sqlite3.Row

try:
    rows = conn.execute('SELECT COUNT(*) as c FROM activity_records').fetchone()
    print('activity_records count:', rows['c'])
    rows2 = conn.execute('SELECT club_name, COUNT(*) as cnt FROM activity_records GROUP BY club_name LIMIT 10').fetchall()
    for r in rows2:
        print('  ar:', r['club_name'], r['cnt'])
except Exception as e:
    print('activity_records error:', e)

try:
    rows = conn.execute('SELECT COUNT(*) as c FROM checkin_sessions').fetchone()
    print('checkin_sessions count:', rows['c'])
    rows2 = conn.execute('SELECT club_name, COUNT(*) as cnt FROM checkin_sessions GROUP BY club_name LIMIT 10').fetchall()
    for r in rows2:
        print('  cs:', r['club_name'], r['cnt'])
except Exception as e:
    print('checkin_sessions error:', e)

tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()
print('All tables:', [t['name'] for t in tables])

conn.close()
