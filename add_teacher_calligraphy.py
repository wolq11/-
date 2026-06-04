import sqlite3
conn = sqlite3.connect('data/club_stats.db')

try:
    conn.execute("INSERT INTO club_teachers (club_name, teacher_name, introduction, created_at) VALUES (?, ?, ?, datetime('now','localtime'))",
                 ('书法协会', '孙七', '书法协会指导老师，负责社团整体指导与管理工作。'))
    print('Added teacher: 孙七 -> 书法协会')
except Exception as e:
    print(f'Error: {e}')

conn.commit()

print('\n=== Verify ===')
rows = conn.execute("SELECT club_name, teacher_name FROM club_teachers WHERE club_name='书法协会'").fetchall()
for r in rows:
    print(r)

conn.close()
print('Done!')
