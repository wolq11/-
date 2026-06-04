import sqlite3, sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
DB = 'd:/work2/club-stats/data/club_stats.db'
conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row

# Check students with empty club_name
rows = conn.execute("SELECT id, username, club_name FROM users WHERE role='student' AND (club_name IS NULL OR club_name='')").fetchall()
print(f"Students with no club_name: {len(rows)}")
for r in rows[:5]:
    print(f"  id={r['id']} username={r['username']}")

# Check students with valid club_name
rows2 = conn.execute("SELECT id, username, club_name FROM users WHERE role='student' AND club_name!='' AND club_name IS NOT NULL LIMIT 5").fetchall()
print(f"\nSample students WITH club_name:")
for r in rows2:
    print(f"  id={r['id']} username={r['username']} club={r['club_name']}")
    # Check if they have workload data
    acts = conn.execute("SELECT COUNT(*) as c FROM checkin_records cr JOIN checkin_sessions cs ON cr.session_id=cs.id WHERE cr.student_name=(SELECT real_name FROM user_profiles WHERE user_id=?) AND cs.club_name=? AND cs.is_completed=1", (r['id'], r['club_name'])).fetchone()
    others = conn.execute("SELECT COUNT(*) as c FROM workload_submissions WHERE student_user_id=? AND club_name=?", (r['id'], r['club_name'])).fetchone()
    print(f"    activities={acts['c']} other_workload={others['c']}")

# Also check: are there checkin_records matching by user_id instead of name?
print("\n\nCheck checkin_records structure:")
cols = conn.execute("PRAGMA table_info(checkin_records)").fetchall()
for c in cols:
    print(f"  {c[1]} ({c[2]})")

conn.close()
