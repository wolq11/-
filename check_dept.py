import sqlite3
conn = sqlite3.connect('data/club_stats.db')
conn.row_factory = sqlite3.Row

rows = conn.execute('SELECT college, COUNT(*) as cnt FROM club_members GROUP BY college').fetchall()
print('club_members college counts:')
for r in rows:
    print('  "' + str(r['college']) + '": ' + str(r['cnt']))

rows2 = conn.execute('SELECT college, COUNT(*) as cnt FROM user_profiles GROUP BY college').fetchall()
print('\nuser_profiles college counts:')
for r in rows2:
    print('  "' + str(r['college']) + '": ' + str(r['cnt']))

conn.close()
