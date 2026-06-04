import sqlite3
conn = sqlite3.connect('data/club_stats.db')

try:
    conn.execute("INSERT INTO club_profiles (club_name, description, star_rating, show_star, president, category, guiding_unit, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now','localtime'), datetime('now','localtime'))",
                 ('校学生会', '校学生会是全校学生自我服务、自我管理、自我教育、自我监督的群众性组织，是学校联系广大同学的桥梁和纽带。我们秉承"全心全意为同学服务"的宗旨，致力于维护学生合法权益，丰富校园文化生活，促进学生全面发展，引领广大同学成长为有理想、有本领、有担当的新时代青年。', 5, 1, 'lina_admin', '综合类', '团委'))
    print('Added 校学生会 to club_profiles')
except Exception as e:
    print(f'Error: {e}')

conn.commit()

print('\n=== club_profiles after fix ===')
rows = conn.execute('SELECT club_name, star_rating, president, category, guiding_unit FROM club_profiles ORDER BY club_name').fetchall()
for r in rows:
    print(dict(r))

conn.close()
print('Done!')
