import sqlite3
conn = sqlite3.connect('data/club_stats.db')

work_ids = {
    '王十二': '2024001',
    '李十九': '2024002',
    '赵六': '2024003',
    '孙七': '2024004',
    '周八': '2024005',
    '吴九': '2024006',
    '郑十': '2024007',
    '钱十一': '2024008',
    '冯十三': '2024009',
    '陈十四': '2024010',
    '褚十五': '2024011',
    '卫十六': '2024012',
    '蒋十七': '2024013',
    '沈十八': '2024014',
}

for name, wid in work_ids.items():
    cursor = conn.execute("UPDATE teacher_profiles SET work_id=? WHERE real_name=?", (wid, name))
    if cursor.rowcount > 0:
        print(f'Updated {name} -> work_id={wid}')
    else:
        print(f'Not found: {name}')

conn.commit()

print('\n=== Verify ===')
rows = conn.execute("SELECT real_name, work_id FROM teacher_profiles WHERE real_name IN ('王十二','李十九','赵六','孙七','周八','吴九','郑十','钱十一','冯十三','陈十四','褚十五','卫十六','蒋十七','沈十八')").fetchall()
for r in rows:
    print(f'{r[0]}: {r[1]}')

conn.close()
print('Done!')
