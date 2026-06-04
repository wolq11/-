import sqlite3
conn = sqlite3.connect('data/club_stats.db')

# 添加指导老师到 club_teachers 表
teachers = [
    ('篮球社', '赵六', '篮球社指导老师，负责社团整体指导与管理工作。'),
    ('校学生会', '王十二', '校学生会指导老师，负责学生会整体指导与管理工作。'),
    ('青年志愿者协会', '李十九', '青年志愿者协会指导老师，负责协会整体指导与管理工作。'),
]

for club, name, intro in teachers:
    try:
        conn.execute("INSERT INTO club_teachers (club_name, teacher_name, introduction, created_at) VALUES (?, ?, ?, datetime('now','localtime'))",
                     (club, name, intro))
        print(f'Added teacher: {name} -> {club}')
    except Exception as e:
        print(f'Error adding {name}: {e}')

conn.commit()

print('\n=== Verify ===')
rows = conn.execute("SELECT club_name, teacher_name, introduction FROM club_teachers WHERE club_name IN ('篮球社','校学生会','青年志愿者协会')").fetchall()
for r in rows:
    print(r)

conn.close()
print('Done!')
