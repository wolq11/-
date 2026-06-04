import sqlite3
conn = sqlite3.connect('data/club_stats.db')

users = [
    (2, 'zhangwei_leader', 'admin123', 'user', '国旗护卫队'),
    (3, 'lina_admin', '888888', 'user', '校学生会'),
    (4, 'wangfang_lead', '123456', 'user', '青年志愿者协会'),
    (5, 'liuqiang_boss', '666666', 'user', '篮球社'),
    (6, 'chenjie_head', 'admin123', 'user', '书法协会'),
    (7, 'yangyang_cap', '111111', 'user', '动漫社'),
    (8, 'zhaomin_pres', '123456', 'user', '广播台'),
    (9, 'huanglei_chief', '000000', 'user', '街舞社'),
    (10, 'wugang_master', 'admin123', 'user', '科技创新社'),
    (11, 'xuting_leader', '123123', 'user', '摄影协会'),
    (12, 'sunwei_admin', '123456', 'user', '辩论队'),
    (13, 'machao_head', '654321', 'user', '吉他社'),
    (14, 'zhuxue_captain', 'admin123', 'user', '足球社'),
    (15, 'huyuan_director', '123456', 'user', '话剧社'),
    (16, 'guolin_manager', '888888', 'user', '文学社'),
]

for uid, username, pwd, role, club in users:
    try:
        conn.execute("INSERT INTO users (id, username, password, role, club_name, created_at) VALUES (?, ?, ?, ?, ?, datetime('now','localtime'))",
                     (uid, username, pwd, role, club))
        conn.execute("INSERT INTO user_profiles (user_id, real_name, student_id) VALUES (?, ?, ?)",
                     (uid, username, '20240109' + str(uid).zfill(3)))
        print(f'Added: {username} -> {club}')
    except Exception as e:
        print(f'Error adding {username}: {e}')

conn.commit()
conn.close()
print('Done!')
