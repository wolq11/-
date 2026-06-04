import sqlite3
conn = sqlite3.connect('data/club_stats.db')
cursor = conn.execute("UPDATE club_profiles SET star_rating=4 WHERE club_name='青年志愿者协会'")
print(f'Updated {cursor.rowcount} row(s)')
conn.commit()

rows = conn.execute("SELECT club_name, star_rating FROM club_profiles WHERE club_name='青年志愿者协会'").fetchall()
for r in rows:
    print(r)

conn.close()
print('Done!')
