import sqlite3
conn = sqlite3.connect('data/club_stats.db')
conn.row_factory = sqlite3.Row

print('=== club_profiles ===')
rows = conn.execute('SELECT club_name, star_rating, president, category, guiding_unit FROM club_profiles ORDER BY club_name').fetchall()
for r in rows:
    print(dict(r))

print('\n=== club_departments for 校学生会 ===')
rows = conn.execute("SELECT dept_name FROM club_departments WHERE club_name='校学生会'").fetchall()
for r in rows:
    print(r['dept_name'])

conn.close()
