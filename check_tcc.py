import server
conn = server.db.get_conn()
members = conn.execute("SELECT real_name, department, class_name, specialty FROM club_members LIMIT 10").fetchall()
for m in members:
    print(dict(m))
conn.close()
