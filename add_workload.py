#!/usr/bin/env python3
import sqlite3
import random
from datetime import datetime, timedelta

DB_PATH = '/opt/liqi/-/data/club_stats.db'

def main():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("SELECT id, username, club_name FROM users WHERE role = 'student' OR role = 'user';")
    students = cursor.fetchall()
    
    cursor.execute("SELECT id, username, club_name FROM users WHERE role = 'teacher' AND club_name IS NOT NULL AND club_name != '';")
    teachers = cursor.fetchall()
    
    club_teachers = {}
    for tid, tname, cname in teachers:
        if cname not in club_teachers:
            club_teachers[cname] = []
        club_teachers[cname].append((tid, tname))
    
    inserted_count = 0
    for student_id, student_name, club_name in students:
        if not club_name or club_name not in club_teachers:
            continue
        
        score = random.randint(1, 10)
        
        reviewer_id, reviewer_name = random.choice(club_teachers[club_name])
        
        created_at = datetime.now() - timedelta(days=random.randint(1, 30))
        reviewed_at = created_at + timedelta(hours=random.randint(1, 24))
        
        cursor.execute('''
            INSERT INTO workload_submissions 
            (student_user_id, student_name, club_name, item_name, score, status, 
             reviewer_id, reviewer_name, review_note, created_at, reviewed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            student_id,
            student_name,
            club_name,
            '其他工作量',
            score,
            'approved',
            reviewer_id,
            reviewer_name,
            '审批通过',
            created_at.strftime('%Y-%m-%d %H:%M:%S'),
            reviewed_at.strftime('%Y-%m-%d %H:%M:%S')
        ))
        inserted_count += 1
    
    conn.commit()
    conn.close()
    
    print(f"已为 {inserted_count} 名学生添加工作量数据")

if __name__ == '__main__':
    main()