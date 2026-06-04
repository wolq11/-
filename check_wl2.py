import sqlite3, requests, sys, io, json
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
API = 'http://localhost:5000'
DB = 'd:/work2/club-stats/data/club_stats.db'

# Find a student with empty club_name who has workload data
conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row
rows = conn.execute("""
    SELECT u.id, u.username, u.password, u.club_name, 
           (SELECT COUNT(*) FROM workload_submissions ws WHERE ws.student_user_id=u.id) as wl_count,
           (SELECT COUNT(*) FROM checkin_records cr WHERE cr.student_name=(SELECT real_name FROM user_profiles WHERE user_id=u.id)) as act_count
    FROM users u 
    WHERE u.role='student' AND (u.club_name IS NULL OR u.club_name='')
    ORDER BY wl_count DESC
    LIMIT 5
""").fetchall()

for r in rows:
    print(f"id={r['id']} user={r['username']} club='{r['club_name']}' wl={r['wl_count']} act={r['act_count']}")

if rows:
    test_user = rows[0]
    print(f"\nTesting with: {test_user['username']} (id={test_user['id']})")
    s = requests.Session()
    r = s.post(f'{API}/api/login', json={'username': test_user['username'], 'password': test_user['password']})
    if r.json().get('success'):
        # Test club-stats
        r = s.get(f'{API}/api/workload/club-stats')
        d = r.json()
        data = d.get('data', [])
        me = next((x for x in data if x['user_id'] == test_user['id']), None)
        print(f"  Found in club-stats: {me is not None}")
        if me:
            print(f"  activity_score={me['activity_score']} other_score={me['other_score']}")
        
        # Test student-detail
        r = s.get(f'{API}/api/workload/student-detail?user_id={test_user['id']}')
        d = r.json()
        print(f"  student-detail: success={d.get('success')}")
        if d.get('success'):
            print(f"  activity_list: {len(d.get('activity_list', []))}")
            print(f"  other_list: {len(d.get('other_list', []))}")
            print(f"  club_name used: '{d.get('student', {}).get('club_name', '')}'")
        else:
            print(f"  error: {d.get('error')}")

conn.close()
