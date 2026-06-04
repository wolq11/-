import sqlite3
conn = sqlite3.connect('data/club_stats.db')

names = ['王五', '王十二', '李十九']
for name in names:
    cursor = conn.execute('UPDATE users SET role=? WHERE username=?', ('teacher', name))
    if cursor.rowcount > 0:
        print(f'Updated {name} -> teacher')
    else:
        print(f'Not found: {name}')

conn.commit()

print('\n=== Verify ===')
rows = conn.execute("SELECT id, username, role, club_name FROM users WHERE username IN ('王五','王十二','李十九')").fetchall()
for r in rows:
    print(r)

conn.close()
print('Done!')
