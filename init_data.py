#!/usr/bin/env python3
import sqlite3
import random
import hashlib
import os
from datetime import datetime, timedelta

DB_PATH = '/opt/liqi/-/data/club_stats.db'

# 常用姓氏和名字
SURNAMES = ['王', '李', '张', '刘', '陈', '杨', '黄', '赵', '周', '吴', '徐', '孙', '马', '朱', '胡', '郭', '何', '林', '罗', '高', '郑', '梁', '谢', '宋', '唐', '许', '邓', '冯', '韩', '曹', '曾', '彭', '萧', '蔡', '潘', '田', '董', '袁', '于', '余', '叶', '蒋', '杜', '苏', '魏', '程', '吕', '丁', '沈', '任', '姚', '卢', '傅', '钟', '姜', '崔', '谭', '廖', '范', '汪', '陆', '金', '石', '戴', '贾', '韦', '夏', '邱', '方', '侯', '邹', '熊', '孟', '秦', '白', '江', '阎', '薛', '尹', '段', '雷', '龙', '史', '陶', '贺', '顾', '毛', '郝', '龚', '邵', '万', '钱', '严', '覃', '武', '戴', '莫', '孔', '向', '常']

GIVEN_NAMES = ['伟', '芳', '娜', '敏', '静', '丽', '强', '磊', '军', '洋', '勇', '艳', '杰', '娟', '涛', '明', '秀', '霞', '平', '刚', '桂', '英', '华', '慧', '建', '文', '斌', '玲', '辉', '萍', '鹏', '红', '琴', '飞', '梅', '鑫', '波', '云', '浩', '宇', '轩', '博', '思', '佳', '欣', '怡', '宁', '睿', '晨', '旭', '阳', '雨', '雪', '梦', '琪', '瑶', '彤', '妍', '薇', '婧', '岚', '翔', '龙', '凤', '麟', '鹤', '燕', '莺', '鹃', '蝶', '莲', '荷', '竹', '菊', '梅', '松', '柏', '枫', '林', '森', '海', '江', '河', '山', '峰', '岳', '川', '星', '月', '光', '辰', '晓', '晴', '露', '霜']

CLASSES = ['计算机科学2301班', '计算机科学2302班', '软件工程2301班', '软件工程2302班', '人工智能2301班', '数据科学2301班', '网络安全2301班', '物联网2301班', '电子信息2301班', '通信工程2301班', '自动化2301班', '机械工程2301班', '土木工程2301班', '建筑学2301班', '经济学2301班', '金融学2301班', '会计学2301班', '工商管理2301班', '市场营销2301班', '法学2301班', '汉语言文学2301班', '英语2301班', '新闻学2301班', '艺术设计2301班', '音乐表演2301班', '舞蹈表演2301班', '体育教育2301班', '数学与应用数学2301班', '物理学2301班', '化学2301班', '生物科学2301班', '医学2301班', '护理学2301班', '药学2301班']

DEPARTMENTS = ['技术部', '宣传部', '组织部', '外联部', '文艺部', '体育部', '秘书处', '财务部', '后勤部', '策划部']

SPECIALTIES = ['摄影', '视频剪辑', '海报设计', '文案写作', '活动策划', '财务管理', '网页设计', '数据分析', '演讲主持', '乐器演奏', '舞蹈', '歌唱', '绘画', '书法', '运动健身']

def generate_name():
    surname = random.choice(SURNAMES)
    given = random.choice(GIVEN_NAMES) + random.choice(GIVEN_NAMES)
    return surname + given

def generate_student_id():
    return '2023' + str(random.randint(10000, 99999))

def generate_phone():
    return '1' + random.choice(['38', '39', '58', '59', '68', '78', '88', '98']) + str(random.randint(10000000, 99999999))

def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()

def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    
    # 4个社团
    clubs = ['羽毛球社', '篮球社', '国旗护卫队', '校礼仪']
    
    # 每个社团的成员数量（20-40不等）
    club_member_counts = {
        '羽毛球社': random.randint(25, 35),
        '篮球社': random.randint(25, 35),
        '国旗护卫队': random.randint(20, 30),
        '校礼仪': random.randint(20, 30)
    }
    
    print("开始创建数据...")
    
    # 1. 创建社团
    for club in clubs:
        try:
            conn.execute('INSERT INTO club_profiles (club_name, description, star_rating) VALUES (?, ?, ?)', 
                        (club, f'{club}是一个充满活力的学生社团', random.randint(1, 3)))
            print(f"创建社团: {club}")
        except sqlite3.IntegrityError:
            print(f"社团已存在: {club}")
    
    # 2. 设置张三为羽毛球社团负责人
    conn.execute('UPDATE users SET club_name=? WHERE username=?', ('羽毛球社', '张三'))
    print("张三设置为羽毛球社团负责人")
    
    # 3. 创建学生用户
    total_students_needed = sum(club_member_counts.values())
    # 由于一个学生可以加入两个社团，实际需要的学生数量约为一半
    actual_students_needed = int(total_students_needed * 0.6)  # 约60%的学生加入一个社团，40%加入两个
    
    students = []
    used_names = set()
    used_ids = set()
    
    # 获取现有用户ID最大值
    max_id = conn.execute('SELECT MAX(id) as m FROM users').fetchone()['m'] or 0
    
    for i in range(actual_students_needed):
        name = generate_name()
        while name in used_names:
            name = generate_name()
        used_names.add(name)
        
        student_id = generate_student_id()
        while student_id in used_ids:
            student_id = generate_student_id()
        used_ids.add(student_id)
        
        password = ''.join(random.choices('abcdefghijklmnopqrstuvwxyz0123456789', k=6))
        class_name = random.choice(CLASSES)
        phone = generate_phone()
        
        user_id = max_id + i + 1
        students.append({
            'id': user_id,
            'username': name,
            'password': password,
            'student_id': student_id,
            'class_name': class_name,
            'phone': phone
        })
    
    # 插入学生用户
    for s in students:
        conn.execute('INSERT INTO users (id, username, password, role, club_name) VALUES (?, ?, ?, ?, ?)',
                    (s['id'], s['username'], hash_password(s['password']), 'student', ''))
        conn.execute('INSERT INTO user_profiles (user_id, real_name, student_id, class_name, phone) VALUES (?, ?, ?, ?, ?)',
                    (s['id'], s['username'], s['student_id'], s['class_name'], s['phone']))
    
    print(f"创建了 {len(students)} 个学生用户")
    
    # 4. 分配学生到社团（每人最多2个社团）
    # 创建社团成员分配
    club_assignments = {club: [] for club in clubs}
    student_club_count = {s['id']: 0 for s in students}
    
    # 先确保每个社团有足够的成员
    student_pool = students.copy()
    random.shuffle(student_pool)
    
    for club in clubs:
        needed = club_member_counts[club]
        assigned = 0
        
        # 从池中选择学生
        for s in student_pool:
            if student_club_count[s['id']] < 2 and assigned < needed:
                club_assignments[club].append(s)
                student_club_count[s['id']] += 1
                assigned += 1
        
        # 如果不够，再从已加入一个社团的学生中选择
        if assigned < needed:
            for s in student_pool:
                if student_club_count[s['id']] == 1 and assigned < needed:
                    club_assignments[club].append(s)
                    student_club_count[s['id']] += 1
                    assigned += 1
    
    # 5. 创建报名申请记录和社团成员记录
    base_time = datetime.now() - timedelta(days=30)
    
    reg_id = conn.execute('SELECT MAX(id) as m FROM club_registrations').fetchone()['m'] or 0
    member_id = conn.execute('SELECT MAX(id) as m FROM club_members').fetchone()['m'] or 0
    
    for club, members in club_assignments.items():
        print(f"{club}: {len(members)} 名成员")
        
        for idx, s in enumerate(members):
            # 创建报名申请记录（已审批通过）
            reg_id += 1
            apply_time = base_time + timedelta(days=random.randint(1, 20))
            review_time = apply_time + timedelta(days=random.randint(1, 5))
            
            department = random.choice(DEPARTMENTS) if random.random() > 0.3 else ''
            specialty = random.choice(SPECIALTIES) if random.random() > 0.5 else ''
            
            conn.execute('''INSERT INTO club_registrations 
                        (id, club_name, student_name, student_phone, student_class, student_id_num, 
                         specialty, department, user_id, status, form_data, created_at, reviewed_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                        (reg_id, club, s['username'], s['phone'], s['class_name'], s['student_id'],
                         specialty, department, s['id'], 'approved', '{}', 
                         apply_time.strftime('%Y-%m-%d %H:%M:%S'), 
                         review_time.strftime('%Y-%m-%d %H:%M:%S')))
            
            # 创建社团成员记录
            member_id += 1
            join_time = review_time
            
            conn.execute('''INSERT INTO club_members 
                        (id, club_name, user_id, username, real_name, student_id_num, class_name, 
                         phone, department, specialty, source, joined_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                        (member_id, club, s['id'], s['username'], s['username'], s['student_id'],
                         s['class_name'], s['phone'], department, specialty, 'registration',
                         join_time.strftime('%Y-%m-%d %H:%M:%S')))
    
    conn.commit()
    
    # 6. 创建社团工具测试数据（接龙、报名、投票、问卷）
    import json as _json
    
    # 获取羽毛球社的成员
    ym_members = conn.execute('SELECT user_id, username, real_name, student_id_num FROM club_members WHERE club_name="羽毛球社" AND user_id!=0').fetchall()
    ym_user_ids = [m['user_id'] for m in ym_members]
    ym_usernames = [m['username'] for m in ym_members]
    
    # 获取羽毛球社负责人
    ym_leader = conn.execute('SELECT id, username FROM users WHERE club_name="羽毛球社" AND role="user"').fetchone()
    
    now = datetime.now()
    deadline_future = (now + timedelta(days=7)).strftime('%Y-%m-%dT%H:%M')
    
    # 6.1 接龙："羽林争霸"社团内部交流
    chain_participants = []
    chain_results = {}
    chain_members = random.sample(ym_members, min(8, len(ym_members)))
    for idx, m in enumerate(chain_members):
        chain_participants.append({
            'username': m['username'],
            'time': (now - timedelta(hours=random.randint(1, 48))).strftime('%Y-%m-%d %H:%M'),
            'content': f'参加第{idx+1}场交流赛'
        })
    
    conn.execute('''INSERT INTO club_tools (club_name, tool_type, title, description, options, vote_mode, limit_count, format_hint, deadline, status, results, participants, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                ('羽毛球社', 'chain', '羽林争霸', '社团内部交流赛接龙，欢迎所有社员参与！',
                 '[]', 'single', 20, '', deadline_future, 'active',
                 _json.dumps(chain_results, ensure_ascii=False),
                 _json.dumps(chain_participants, ensure_ascii=False),
                 (now - timedelta(days=2)).strftime('%Y-%m-%d %H:%M:%S')))
    chain_id = conn.execute('SELECT last_insert_rowid()').fetchone()[0]
    print(f"创建接龙: 羽林争霸 (ID:{chain_id}, {len(chain_participants)}人参与)")
    
    # 给羽毛球社成员发送接龙通知
    for m in ym_members:
        if m['user_id'] and m['user_id'] not in [p.get('username') for p in chain_participants]:
            conn.execute('INSERT INTO notifications (user_id, title, content, type, link, is_read) VALUES (?, ?, ?, ?, ?, 0)',
                        (m['user_id'], '📋 新的接龙：羽林争霸', '羽毛球社发布了新的接龙活动，快来参与吧！', 'tool', f'/api/tool-page/{chain_id}'))
    
    # 6.2 报名："新生杯"羽毛球赛工作人员（5人）
    signup_participants = []
    signup_members = random.sample(ym_members, min(5, len(ym_members)))
    roles = ['裁判', '计分员', '场地管理', '器材管理', '后勤保障']
    for idx, m in enumerate(signup_members):
        signup_participants.append({
            'username': m['username'],
            'time': (now - timedelta(hours=random.randint(1, 36))).strftime('%Y-%m-%d %H:%M'),
            'info': roles[idx] if idx < len(roles) else '工作人员'
        })
    
    conn.execute('''INSERT INTO club_tools (club_name, tool_type, title, description, options, vote_mode, limit_count, format_hint, deadline, status, results, participants, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                ('羽毛球社', 'signup', '新生杯羽毛球赛工作人员', '新生杯羽毛球赛需要5名工作人员协助赛事组织，欢迎报名！',
                 '[]', 'single', 5, '', deadline_future, 'active',
                 _json.dumps({}, ensure_ascii=False),
                 _json.dumps(signup_participants, ensure_ascii=False),
                 (now - timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S')))
    signup_id = conn.execute('SELECT last_insert_rowid()').fetchone()[0]
    print(f"创建报名: 新生杯工作人员 (ID:{signup_id}, {len(signup_participants)}人报名)")
    
    # 给未报名的成员发送通知
    signup_usernames = [p['username'] for p in signup_participants]
    for m in ym_members:
        if m['user_id'] and m['username'] not in signup_usernames:
            conn.execute('INSERT INTO notifications (user_id, title, content, type, link, is_read) VALUES (?, ?, ?, ?, ?, 0)',
                        (m['user_id'], '📋 新的报名：新生杯羽毛球赛工作人员', '羽毛球社发布了新的报名活动，快来参与吧！', 'tool', f'/api/tool-page/{signup_id}'))
    
    # 6.3 投票：社团定制队服颜色/款式意向调查
    vote_options = ['白色短袖+蓝色镶边', '黑色短袖+金色Logo', '蓝色短袖+白色条纹', '红色短袖+黑色下摆', '灰色短袖+蓝色字母']
    vote_results = {}
    vote_participants = []
    for idx, m in enumerate(ym_members):
        choice = random.randint(0, len(vote_options) - 1)
        ck = str(choice)
        vote_results[ck] = vote_results.get(ck, 0) + 1
        vote_participants.append({
            'username': m['username'],
            'time': (now - timedelta(hours=random.randint(1, 72))).strftime('%Y-%m-%d %H:%M'),
            'choice': choice,
            'option': vote_options[choice]
        })
    
    conn.execute('''INSERT INTO club_tools (club_name, tool_type, title, description, options, vote_mode, limit_count, format_hint, deadline, status, results, participants, anonymous, show_counts, results_visible, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                ('羽毛球社', 'vote', '社团定制队服颜色/款式意向调查', '为社团定制统一队服，请投出你最喜欢的款式！',
                 _json.dumps(vote_options, ensure_ascii=False), 'single', 0, '', deadline_future, 'active',
                 _json.dumps(vote_results, ensure_ascii=False),
                 _json.dumps(vote_participants, ensure_ascii=False),
                 0, 1, 1,
                 (now - timedelta(days=3)).strftime('%Y-%m-%d %H:%M:%S')))
    vote_id = conn.execute('SELECT last_insert_rowid()').fetchone()[0]
    print(f"创建投票: 队服颜色款式意向 (ID:{vote_id}, {len(vote_participants)}人投票)")
    
    # 6.4 问卷：羽毛球社成员满意度及需求调查（10人参与）
    survey_questions = [
        '你对社团整体活动安排满意吗？',
        '你觉得社团活动频率如何？',
        '你最希望增加哪类活动？',
        '你对社团场地安排满意吗？',
        '你对社团管理有什么建议？'
    ]
    survey_answers_pool = {
        0: ['非常满意', '满意', '一般', '不太满意'],
        1: ['太多了', '刚好', '有点少', '太少了'],
        2: ['比赛类', '训练类', '社交类', '观赛类'],
        3: ['非常满意', '满意', '一般', '不太满意'],
        4: ['多组织比赛', '改善场地', '增加训练', '加强沟通']
    }
    survey_participants = []
    survey_members = random.sample(ym_members, min(10, len(ym_members)))
    for m in survey_members:
        answers = {}
        for qi in range(len(survey_questions)):
            answers[str(qi)] = random.choice(survey_answers_pool[qi])
        survey_participants.append({
            'username': m['username'],
            'time': (now - timedelta(hours=random.randint(1, 48))).strftime('%Y-%m-%d %H:%M'),
            'answers': answers
        })
    
    conn.execute('''INSERT INTO club_tools (club_name, tool_type, title, description, options, vote_mode, limit_count, format_hint, deadline, status, results, participants, per_user_limit, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                ('羽毛球社', 'survey', '羽毛球社成员满意度及需求调查', '为了更好地服务社员，请认真填写本问卷，您的意见对我们非常重要！',
                 _json.dumps(survey_questions, ensure_ascii=False), 'single', 0, '', deadline_future, 'active',
                 _json.dumps({}, ensure_ascii=False),
                 _json.dumps(survey_participants, ensure_ascii=False),
                 1,
                 (now - timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S')))
    survey_id = conn.execute('SELECT last_insert_rowid()').fetchone()[0]
    print(f"创建问卷: 满意度及需求调查 (ID:{survey_id}, {len(survey_participants)}人填写)")
    
    # 给未参与的成员发送问卷通知
    survey_usernames = [p['username'] for p in survey_participants]
    for m in ym_members:
        if m['user_id'] and m['username'] not in survey_usernames:
            conn.execute('INSERT INTO notifications (user_id, title, content, type, link, is_read) VALUES (?, ?, ?, ?, ?, 0)',
                        (m['user_id'], '📋 新的问卷调查：羽毛球社成员满意度及需求调查', '羽毛球社发布了新的问卷调查，快来参与吧！', 'tool', f'/api/tool-page/{survey_id}'))
    
    conn.commit()
    
    # 输出统计
    print("\n=== 数据创建完成 ===")
    print(f"社团数量: {len(clubs)}")
    for club in clubs:
        count = len(club_assignments[club])
        print(f"  {club}: {count} 人")
    
    print(f"\n学生总数: {len(students)}")
    
    # 统计加入1个和2个社团的学生
    one_club = sum(1 for c in student_club_count.values() if c == 1)
    two_club = sum(1 for c in student_club_count.values() if c == 2)
    print(f"  加入1个社团: {one_club} 人")
    print(f"  加入2个社团: {two_club} 人")
    
    # 输出部分学生账号信息
    print("\n部分学生账号（前10个）:")
    for s in students[:10]:
        print(f"  姓名: {s['username']}, 密码: {s['password']}, 学号: {s['student_id']}, 班级: {s['class_name']}")
    
    conn.close()

if __name__ == '__main__':
    main()