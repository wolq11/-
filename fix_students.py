import sqlite3
conn = sqlite3.connect('data/club_stats.db')

students = [
    ('huyuan014', '654321', 'student', '20240109014'),
    ('guolin015', '123456', 'student', '20240109015'),
]

for username, pwd, role, student_id in students:
    try:
        conn.execute("INSERT INTO users (username, password, role, created_at) VALUES (?, ?, ?, datetime('now','localtime'))",
                     (username, pwd, role))
        uid = conn.execute('SELECT id FROM users WHERE username=?', (username,)).fetchone()[0]
        conn.execute('INSERT INTO user_profiles (user_id, real_name, student_id) VALUES (?, ?, ?)',
                     (uid, username, student_id))
        print(f'Added student: {username} (id={uid}) -> {student_id}')
    except Exception as e:
        print(f'Error adding {username}: {e}')

conn.commit()
conn.close()
print('Done!')
