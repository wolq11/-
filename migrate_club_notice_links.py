import sqlite3, re, sys
sys.stdout.reconfigure(encoding='utf-8')
conn = sqlite3.connect('data/club_stats.db')
conn.row_factory = sqlite3.Row
rows = conn.execute("SELECT id, content FROM notifications WHERE type='club_notice' AND link='/dashboard.html'").fetchall()
updated = 0
for r in rows:
    content = r['content'] or ''
    m = re.search(r'发布了新通知：(.+)$', content)
    if m:
        title = m.group(1).strip()
        cn = conn.execute('SELECT id FROM club_notices WHERE title=? ORDER BY id DESC LIMIT 1', (title,)).fetchone()
        if cn:
            conn.execute('UPDATE notifications SET link=? WHERE id=?', ('club_notice:' + str(cn['id']), r['id']))
            updated += 1
conn.commit()
print('Updated:', updated)
conn.close()
