import sqlite3
conn = sqlite3.connect('data/club_stats.db')

teachers = [
    ('王十二', '123456', '校学生会'),
    ('李十九', '234567', '青年志愿者协会'),
    ('赵六', '345678', '篮球社'),
    ('孙七', '456789', '书法协会'),
    ('周八', '567890', '动漫社'),
    ('吴九', '678901', '广播台'),
    ('郑十', '789012', '街舞社'),
    ('钱十一', '890123', '科技创新社'),
    ('冯十三', '901234', '摄影协会'),
    ('陈十四', '012345', '辩论队'),
    ('褚十五', '111222', '吉他社'),
    ('卫十六', '222333', '足球社'),
    ('蒋十七', '333444', '话剧社'),
    ('沈十八', '444555', '文学社'),
]

for name, pwd, club in teachers:
    try:
        conn.execute("INSERT INTO users (username, password, role, club_name, created_at) VALUES (?, ?, ?, ?, datetime('now','localtime'))",
                     (name, pwd, 'teacher', club))
        uid = conn.execute('SELECT id FROM users WHERE username=?', (name,)).fetchone()[0]
        conn.execute("INSERT INTO teacher_profiles (user_id, real_name, phone, email, introduction, updated_at) VALUES (?, ?, ?, ?, ?, datetime('now','localtime'))",
                     (uid, name, '', '', ''))
        conn.execute("INSERT INTO teacher_clubs (user_id, club_name, created_at) VALUES (?, ?, datetime('now','localtime'))",
                     (uid, club))
        print(f'Added teacher: {name} -> {club} (id={uid})')
    except Exception as e:
        print(f'Error adding {name}: {e}')

conn.commit()
conn.close()
print('Done!')
