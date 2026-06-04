import sqlite3
conn = sqlite3.connect('d:/work2/club-stats/data/club_stats.db')
conn.row_factory = sqlite3.Row

print('=== 名称不一致检查 ===')
act_clubs = [r['club_name'] for r in conn.execute('SELECT DISTINCT club_name FROM activity_records').fetchall()]
guid_clubs = [r['club_name'] for r in conn.execute('SELECT DISTINCT club_name FROM guidance_records').fetchall()]

print('\n活动记录中含"百课"的:')
for c in act_clubs:
    if '百课' in c:
        print('  [%s]' % c)

print('\n指导记录中含"百课"的:')
for c in guid_clubs:
    if '百课' in c:
        print('  [%s]' % c)

print('\n活动记录中含"儿童"的:')
for c in act_clubs:
    if '儿童' in c:
        print('  [%s]' % c)

print('\n指导记录中含"儿童"的:')
for c in guid_clubs:
    if '儿童' in c:
        print('  [%s]' % c)

# Check normalize
import sys
sys.path.insert(0, 'd:/work2/club-stats')
from server import normalize_name, name_similarity

for c1 in act_clubs:
    for c2 in guid_clubs:
        if '百课' in c1 and '百课' in c2:
            n1 = normalize_name(c1)
            n2 = normalize_name(c2)
            sim = name_similarity(n1, n2)
            print('\n  normalize(%s) = %s' % (c1, n1))
            print('  normalize(%s) = %s' % (c2, n2))
            print('  similarity = %.2f' % sim)

conn.close()
