import sqlite3
conn = sqlite3.connect('data/club_stats.db')

students = [
    (17, 'zhangwei001', '123456', 'student', '20240109001'),
    (18, 'lina002', '123456', 'student', '20240109002'),
    (19, 'wangfang003', '888888', 'student', '20240109003'),
    (20, 'liuqiang004', '123456', 'student', '20240109004'),
    (21, 'chenjie005', '666666', 'student', '20240109005'),
    (22, 'yangyang006', '123456', 'student', '20240109006'),
    (23, 'zhaomin007', '111111', 'student', '20240109007'),
    (24, 'huanglei008', '123456', 'student', '20240109008'),
    (25, 'wugang009', '000000', 'student', '20240109009'),
    (26, 'xuting010', '123456', 'student', '20240109010'),
    (27, 'sunwei011', '123123', 'student', '20240109011'),
    (28, 'machao012', '123456', 'student', '20240109012'),
    (29, 'zhuxue013', '123456', 'student', '20240109013'),
    (30, 'huyuan014', '654321', 'student', '20240109014'),
    (31, 'guolin015', '123456', 'student', '20240109015'),
]

for uid, username, pwd, role, student_id in students:
    try:
        conn.execute("INSERT INTO users (id, username, password, role, created_at) VALUES (?, ?, ?, ?, datetime('now','localtime'))",
                     (uid, username, pwd, role))
        conn.execute("INSERT INTO user_profiles (user_id, real_name, student_id) VALUES (?, ?, ?)",
                     (uid, username, student_id))
        print(f'Added student: {username} -> {student_id}')
    except Exception as e:
        print(f'Error adding {username}: {e}')

conn.commit()
conn.close()
print('Done!')
