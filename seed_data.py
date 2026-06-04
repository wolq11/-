import sqlite3
import random
import string
from datetime import datetime, timedelta

DB_PATH = r'd:\work2\club-stats\data\club_stats.db'

CLUBS = [
    {"name": "书法社团", "guiding_unit": "人文学院", "description": "传承书法艺术，弘扬传统文化，提升书写审美能力"},
    {"name": "人工智能社团", "guiding_unit": "信息工程学院", "description": "探索AI前沿技术，开展智能项目实践与创新"},
    {"name": "篮球社", "guiding_unit": "体育学院", "description": "强健体魄，团结协作，提升篮球竞技水平"},
]

USERS = {
    "admin": {"username": "admin", "password": "123456", "role": "admin", "club_name": None},
    "leader1": {"username": "leader1", "password": "123456", "role": "user", "club_name": "书法社团"},
    "leader2": {"username": "leader2", "password": "123456", "role": "user", "club_name": "人工智能社团"},
    "leader3": {"username": "leader3", "password": "123456", "role": "user", "club_name": "篮球社"},
    "teacher1": {"username": "teacher1", "password": "123456", "role": "teacher", "club_name": "书法社团"},
    "teacher2": {"username": "teacher2", "password": "123456", "role": "teacher", "club_name": "篮球社"},
    "student1": {"username": "student1", "password": "123456", "role": "student", "club_name": None},
    "student2": {"username": "student2", "password": "123456", "role": "student", "club_name": None},
    "student3": {"username": "student3", "password": "123456", "role": "student", "club_name": None},
    "student4": {"username": "student4", "password": "123456", "role": "student", "club_name": None},
    "student5": {"username": "student5", "password": "123456", "role": "student", "club_name": None},
}

TEACHER_PROFILES = {
    "teacher1": {"work_id": "T20210001", "real_name": "王文轩", "phone": "13800001001", "email": "wangwx@edu.cn", "introduction": "书法专业副教授，擅长楷书与行书，从事书法教育15年"},
    "teacher2": {"work_id": "T20210002", "real_name": "李强", "phone": "13800001002", "email": "liqiang@edu.cn", "introduction": "体育学院讲师，国家二级篮球裁判，带队经验丰富"},
}

TEACHER_CLUBS = [
    ("teacher1", "书法社团"),
    ("teacher1", "人工智能社团"),
    ("teacher2", "篮球社"),
]

STUDENT_PROFILES = {
    "student1": {"real_name": "张明", "student_id": "20240101001", "age": "20", "class_name": "2024级计算机1班", "phone": "13900001001", "email": "zhangming@stu.edu.cn"},
    "student2": {"real_name": "刘婷", "student_id": "20240101002", "age": "19", "class_name": "2024级计算机2班", "phone": "13900001002", "email": "liuting@stu.edu.cn"},
    "student3": {"real_name": "陈浩", "student_id": "20240102001", "age": "20", "class_name": "2024级软件工程1班", "phone": "13900001003", "email": "chenhao@stu.edu.cn"},
    "student4": {"real_name": "赵雪", "student_id": "20240102002", "age": "19", "class_name": "2024级数据科学1班", "phone": "13900001004", "email": "zhaoxue@stu.edu.cn"},
    "student5": {"real_name": "孙磊", "student_id": "20240103001", "age": "21", "class_name": "2024级体育教育1班", "phone": "13900001005", "email": "sunlei@stu.edu.cn"},
}

ACTIVITIES = [
    {"club_name": "书法社团", "activity_name": "楷书基础技法培训", "location_name": "艺术楼B201", "activity_content": "学习楷书基本笔画与结构", "plan_text": "1.讲解楷书基本笔画 2.示范横竖撇捺 3.学员练习 4.点评指导"},
    {"club_name": "书法社团", "activity_name": "行书临摹与创作", "location_name": "艺术楼B201", "activity_content": "临摹经典行书作品并进行创作", "plan_text": "1.赏析《兰亭序》 2.临摹练习 3.自由创作 4.作品展示"},
    {"club_name": "人工智能社团", "activity_name": "Python机器学习入门", "location_name": "信息楼A305", "activity_content": "学习Python基础与机器学习算法", "plan_text": "1.Python环境搭建 2.数据处理基础 3.scikit-learn实践 4.项目实战"},
    {"club_name": "人工智能社团", "activity_name": "深度学习项目实战", "location_name": "信息楼A305", "activity_content": "使用PyTorch完成图像分类项目", "plan_text": "1.PyTorch框架介绍 2.CNN原理讲解 3.代码实现 4.模型调优"},
    {"club_name": "篮球社", "activity_name": "3v3篮球对抗赛", "location_name": "体育馆篮球场", "activity_content": "组织3v3半场对抗赛，锻炼实战能力", "plan_text": "1.热身训练 2.战术讲解 3.分组对抗 4.赛后总结"},
]

UPLOAD_GROUPS = [
    {"club_name": "书法社团", "items": [
        {"file_name": "书法比赛获奖证书.jpg", "file_type": "image", "description": "省级书法大赛一等奖", "category": "honor", "status": "approved"},
        {"file_name": "书法展览活动照片.jpg", "file_type": "image", "description": "校园书法展览现场", "category": "activity", "status": "approved"},
        {"file_name": "创新书法教学方案.pdf", "file_type": "document", "description": "数字化书法教学创新方案", "category": "innovation", "status": "pending"},
    ]},
    {"club_name": "人工智能社团", "items": [
        {"file_name": "AI竞赛获奖证明.jpg", "file_type": "image", "description": "全国大学生AI创新大赛银奖", "category": "honor", "status": "approved"},
        {"file_name": "AI讲座活动记录.pdf", "file_type": "document", "description": "人工智能前沿讲座活动总结", "category": "activity", "status": "pending"},
    ]},
    {"club_name": "篮球社", "items": [
        {"file_name": "篮球联赛冠军奖杯.jpg", "file_type": "image", "description": "校级篮球联赛冠军", "category": "honor", "status": "approved"},
        {"file_name": "篮球训练营合影.jpg", "file_type": "image", "description": "暑期篮球训练营活动照片", "category": "activity", "status": "approved"},
        {"file_name": "智能训练系统设计.pdf", "file_type": "document", "description": "基于AI的篮球训练辅助系统", "category": "innovation", "status": "pending"},
    ]},
]

ADMIN_NOTIFICATIONS = [
    {"title": "关于开展社团年度考核的通知", "content": "各社团请于本月底前完成年度考核材料提交，包括活动记录、成员名单、财务报表等。", "target_type": "all", "target_club": ""},
    {"title": "社团活动场地预约须知", "content": "即日起社团活动场地需提前3天在线预约，请各社团负责人及时提交申请。", "target_type": "all", "target_club": ""},
    {"title": "书法社团材料审核通过", "content": "书法社团提交的荣誉材料已审核通过，请查看详情。", "target_type": "club", "target_club": "书法社团"},
    {"title": "人工智能社团招募审批结果", "content": "人工智能社团的招募申请已批准，可开展招新活动。", "target_type": "club", "target_club": "人工智能社团"},
]

RECRUITMENTS = [
    {"club_name": "书法社团", "title": "书法社团2026年春季招新", "description": "欢迎对书法感兴趣的同学加入我们，零基础也可报名，提供专业指导。", "recruit_type": "member", "max_count": 20, "status": "approved", "deadline": "2026-06-30"},
    {"club_name": "人工智能社团", "title": "AI社团技术骨干招募", "description": "招募有编程基础的同学，参与AI项目开发与竞赛。", "recruit_type": "member", "max_count": 15, "status": "approved", "deadline": "2026-06-15"},
    {"club_name": "篮球社", "title": "篮球社新成员招募", "description": "热爱篮球运动的同学均可报名，不限技术水平。", "recruit_type": "member", "max_count": 25, "status": "pending", "deadline": "2026-07-01"},
]

FINANCE_RECORDS = [
    {"club_name": "书法社团", "type": "income", "category": "学校拨款", "amount": 2000.00, "description": "2026年春季学期社团活动经费", "record_date": "2026-04-15", "recorder": "王文轩"},
    {"club_name": "书法社团", "type": "expense", "category": "活动物资", "amount": 580.00, "description": "购买宣纸、毛笔、墨汁等书法用品", "record_date": "2026-04-20", "recorder": "王文轩"},
    {"club_name": "书法社团", "type": "expense", "category": "场地费用", "amount": 200.00, "description": "艺术楼活动场地使用费", "record_date": "2026-05-01", "recorder": "王文轩"},
    {"club_name": "人工智能社团", "type": "income", "category": "学校拨款", "amount": 3000.00, "description": "2026年春季学期社团活动经费", "record_date": "2026-04-10", "recorder": "李强"},
    {"club_name": "人工智能社团", "type": "expense", "category": "设备采购", "amount": 1500.00, "description": "购买GPU服务器使用时长", "record_date": "2026-04-18", "recorder": "李强"},
    {"club_name": "人工智能社团", "type": "income", "category": "竞赛奖金", "amount": 800.00, "description": "AI创新大赛奖金", "record_date": "2026-05-05", "recorder": "李强"},
    {"club_name": "篮球社", "type": "income", "category": "学校拨款", "amount": 1500.00, "description": "2026年春季学期社团活动经费", "record_date": "2026-04-12", "recorder": "李强"},
    {"club_name": "篮球社", "type": "expense", "category": "活动物资", "amount": 650.00, "description": "购买篮球、队服等运动器材", "record_date": "2026-04-22", "recorder": "李强"},
    {"club_name": "篮球社", "type": "expense", "category": "场地费用", "amount": 300.00, "description": "体育馆场地租赁费", "record_date": "2026-05-03", "recorder": "李强"},
]

CLUB_MEMBERS_DATA = [
    {"club_name": "书法社团", "real_name": "张明", "student_id_num": "20240101001", "class_name": "2024级计算机1班", "phone": "13900001001", "source": "registration"},
    {"club_name": "书法社团", "real_name": "刘婷", "student_id_num": "20240101002", "class_name": "2024级计算机2班", "phone": "13900001002", "source": "registration"},
    {"club_name": "书法社团", "real_name": "赵雪", "student_id_num": "20240102002", "class_name": "2024级数据科学1班", "phone": "13900001004", "source": "recruitment"},
    {"club_name": "人工智能社团", "real_name": "张明", "student_id_num": "20240101001", "class_name": "2024级计算机1班", "phone": "13900001001", "source": "registration"},
    {"club_name": "人工智能社团", "real_name": "陈浩", "student_id_num": "20240102001", "class_name": "2024级软件工程1班", "phone": "13900001003", "source": "registration"},
    {"club_name": "人工智能社团", "real_name": "赵雪", "student_id_num": "20240102002", "class_name": "2024级数据科学1班", "phone": "13900001004", "source": "recruitment"},
    {"club_name": "篮球社", "real_name": "陈浩", "student_id_num": "20240102001", "class_name": "2024级软件工程1班", "phone": "13900001003", "source": "registration"},
    {"club_name": "篮球社", "real_name": "孙磊", "student_id_num": "20240103001", "class_name": "2024级体育教育1班", "phone": "13900001005", "source": "registration"},
    {"club_name": "篮球社", "real_name": "刘婷", "student_id_num": "20240101002", "class_name": "2024级计算机2班", "phone": "13900001002", "source": "recruitment"},
]


def random_code(length=6):
    return ''.join(random.choices(string.digits, k=length))


def random_token(length=12):
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=length))


def random_datetime(days_back=30):
    now = datetime.now()
    offset = random.randint(0, days_back * 24 * 60)
    dt = now - timedelta(minutes=offset)
    return dt.strftime('%Y-%m-%d %H:%M:%S')


def random_date_str(days_back=30):
    now = datetime.now()
    offset = random.randint(0, days_back)
    dt = now - timedelta(days=offset)
    return dt.strftime('%Y-%m-%d')


def get_teacher_for_club(club_name):
    mapping = {
        "书法社团": "teacher1",
        "人工智能社团": "teacher1",
        "篮球社": "teacher2",
    }
    return mapping.get(club_name)


def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    stats = {}

    existing_users = {r['username'] for r in cur.execute('SELECT username FROM users').fetchall()}
    user_ids = {}

    inserted_users = 0
    for key, info in USERS.items():
        if info['username'] in existing_users:
            row = cur.execute('SELECT id FROM users WHERE username=?', (info['username'],)).fetchone()
            user_ids[key] = row['id']
            continue
        cur.execute(
            'INSERT INTO users (username, password, role, club_name) VALUES (?, ?, ?, ?)',
            (info['username'], info['password'], info['role'], info['club_name'])
        )
        user_ids[key] = cur.lastrowid
        inserted_users += 1
    conn.commit()
    stats['users'] = inserted_users

    inserted_profiles = 0
    for key, profile in STUDENT_PROFILES.items():
        if key not in user_ids:
            continue
        uid = user_ids[key]
        existing = cur.execute('SELECT id FROM user_profiles WHERE user_id=?', (uid,)).fetchone()
        if existing:
            continue
        cur.execute(
            'INSERT INTO user_profiles (user_id, real_name, student_id, age, class_name, phone, email) VALUES (?, ?, ?, ?, ?, ?, ?)',
            (uid, profile['real_name'], profile['student_id'], profile['age'], profile['class_name'], profile['phone'], profile['email'])
        )
        inserted_profiles += 1
    conn.commit()
    stats['user_profiles'] = inserted_profiles

    inserted_teacher_profiles = 0
    for key, profile in TEACHER_PROFILES.items():
        if key not in user_ids:
            continue
        uid = user_ids[key]
        existing = cur.execute('SELECT id FROM teacher_profiles WHERE user_id=?', (uid,)).fetchone()
        if existing:
            continue
        cur.execute(
            'INSERT INTO teacher_profiles (user_id, work_id, real_name, phone, email, introduction) VALUES (?, ?, ?, ?, ?, ?)',
            (uid, profile['work_id'], profile['real_name'], profile['phone'], profile['email'], profile['introduction'])
        )
        inserted_teacher_profiles += 1
    conn.commit()
    stats['teacher_profiles'] = inserted_teacher_profiles

    inserted_teacher_clubs = 0
    for teacher_key, club_name in TEACHER_CLUBS:
        if teacher_key not in user_ids:
            continue
        uid = user_ids[teacher_key]
        existing = cur.execute('SELECT id FROM teacher_clubs WHERE user_id=? AND club_name=?', (uid, club_name)).fetchone()
        if existing:
            continue
        cur.execute(
            'INSERT INTO teacher_clubs (user_id, club_name) VALUES (?, ?)',
            (uid, club_name)
        )
        inserted_teacher_clubs += 1
    conn.commit()
    stats['teacher_clubs'] = inserted_teacher_clubs

    inserted_tokens = 0
    club_tokens = {}
    for club in CLUBS:
        existing = cur.execute('SELECT token FROM club_tokens WHERE club_name=?', (club['name'],)).fetchone()
        if existing:
            club_tokens[club['name']] = existing['token']
            continue
        token = random_token(12)
        cur.execute(
            'INSERT INTO club_tokens (club_name, token) VALUES (?, ?)',
            (club['name'], token)
        )
        club_tokens[club['name']] = token
        inserted_tokens += 1
    conn.commit()
    stats['club_tokens'] = inserted_tokens

    inserted_profiles = 0
    for club in CLUBS:
        existing = cur.execute('SELECT id FROM club_profiles WHERE club_name=?', (club['name'],)).fetchone()
        if existing:
            continue
        cur.execute(
            'INSERT INTO club_profiles (club_name, description, guiding_unit) VALUES (?, ?, ?)',
            (club['name'], club['description'], club['guiding_unit'])
        )
        inserted_profiles += 1
    conn.commit()
    stats['club_profiles'] = inserted_profiles

    student_keys = ["student1", "student2", "student3", "student4", "student5"]
    session_ids = []
    inserted_sessions = 0

    for i, act in enumerate(ACTIVITIES):
        start_dt = datetime.now() - timedelta(days=random.randint(1, 28), hours=random.randint(8, 16))
        end_dt = start_dt + timedelta(hours=random.randint(1, 3))
        start_time = start_dt.strftime('%Y-%m-%d %H:%M:%S')
        end_time = end_dt.strftime('%Y-%m-%d %H:%M:%S')
        activity_time = start_dt.strftime('%Y-%m-%d')
        checkin_code = random_code(6)
        checkout_code = random_code(6)
        created_at = (start_dt - timedelta(hours=random.randint(1, 12))).strftime('%Y-%m-%d %H:%M:%S')

        teacher_key = get_teacher_for_club(act['club_name'])
        teacher_id_str = str(user_ids.get(teacher_key, '')) if teacher_key else ''

        summary_text = f"{act['activity_name']}圆满完成，同学们积极参与，收获颇丰。"

        cur.execute(
            '''INSERT INTO checkin_sessions
            (club_name, activity_name, checkin_code, checkout_code, checkout_method, location_name,
             start_time, end_time, is_completed, activity_time, activity_content, plan_text, plan_path,
             summary_text, summary_path, completion_photo, teacher_ids, warning, warning_reason, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
            (
                act['club_name'], act['activity_name'], checkin_code, checkout_code, 'code',
                act['location_name'], start_time, end_time, 1, activity_time,
                act['activity_content'], act['plan_text'], '', summary_text, '', '',
                teacher_id_str, '', '', created_at
            )
        )
        session_ids.append((cur.lastrowid, act['club_name']))
        inserted_sessions += 1
    conn.commit()
    stats['checkin_sessions'] = inserted_sessions

    inserted_records = 0
    for session_id, club_name in session_ids:
        num_students = random.randint(3, 5)
        participants = random.sample(student_keys, num_students)
        for sk in participants:
            uid = user_ids.get(sk)
            profile = STUDENT_PROFILES.get(sk, {})
            student_name = profile.get('real_name', sk)
            student_class = profile.get('class_name', '')
            student_id = profile.get('student_id', '')
            checkin_method = random.choice(['code', 'qrcode'])
            cur.execute(
                'INSERT INTO checkin_records (session_id, club_name, student_name, student_class, student_id, checkin_method) VALUES (?, ?, ?, ?, ?, ?)',
                (session_id, club_name, student_name, student_class, student_id, checkin_method)
            )
            inserted_records += 1
    conn.commit()
    stats['checkin_records'] = inserted_records

    inserted_teacher_cc = 0
    for session_id, club_name in session_ids:
        teacher_key = get_teacher_for_club(club_name)
        if not teacher_key or teacher_key not in user_ids:
            continue
        teacher_uid = user_ids[teacher_key]
        existing = cur.execute(
            'SELECT id FROM teacher_checkin_checkout WHERE session_id=? AND teacher_user_id=?',
            (session_id, teacher_uid)
        ).fetchone()
        if existing:
            continue

        sess_row = cur.execute('SELECT start_time, end_time FROM checkin_sessions WHERE id=?', (session_id,)).fetchone()
        checkin_time = sess_row['start_time'] if sess_row else None
        checkout_time = sess_row['end_time'] if sess_row else None

        lat = round(random.uniform(36.60, 36.70), 6)
        lng = round(random.uniform(116.90, 117.00), 6)

        cur.execute(
            '''INSERT INTO teacher_checkin_checkout
            (session_id, teacher_user_id, club_name, checkin_time, checkout_time,
             checkin_lat, checkin_lng, checkout_lat, checkout_lng, checkout_method, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
            (session_id, teacher_uid, club_name, checkin_time, checkout_time,
             lat, lng, lat, lng, 'code', 'checked_out')
        )
        inserted_teacher_cc += 1
    conn.commit()
    stats['teacher_checkin_checkout'] = inserted_teacher_cc

    inserted_uploads = 0
    for group in UPLOAD_GROUPS:
        club_name = group['club_name']
        token = club_tokens.get(club_name, '')
        group_id = random_token(8)
        for item in group['items']:
            file_path = f"uploads/{token}/{random_token(12)}.{item['file_type'][:3]}"
            cur.execute(
                '''INSERT INTO club_uploads
                (club_token, club_name, file_name, file_path, file_type, description, group_id, status, category)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                (token, club_name, item['file_name'], file_path, item['file_type'],
                 item['description'], group_id, item['status'], item['category'])
            )
            inserted_uploads += 1
    conn.commit()
    stats['club_uploads'] = inserted_uploads

    inserted_admin_notifs = 0
    admin_uid = user_ids.get('admin', 1)
    for notif in ADMIN_NOTIFICATIONS:
        cur.execute(
            'INSERT INTO admin_notifications (title, content, target_type, target_club, sender_id) VALUES (?, ?, ?, ?, ?)',
            (notif['title'], notif['content'], notif['target_type'], notif['target_club'], admin_uid)
        )
        inserted_admin_notifs += 1
    conn.commit()
    stats['admin_notifications'] = inserted_admin_notifs

    inserted_recruitments = 0
    recruitment_ids = []
    for rec in RECRUITMENTS:
        leader_key = None
        for uk, uv in USERS.items():
            if uv['club_name'] == rec['club_name'] and uv['role'] == 'user':
                leader_key = uk
                break
        created_by = user_ids.get(leader_key, 1)
        cur.execute(
            '''INSERT INTO recruitments
            (club_name, title, description, recruit_type, max_count, current_count, status, created_by, deadline)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
            (rec['club_name'], rec['title'], rec['description'], rec['recruit_type'],
             rec['max_count'], 0, rec['status'], created_by, rec['deadline'])
        )
        recruitment_ids.append((cur.lastrowid, rec['club_name']))
        inserted_recruitments += 1
    conn.commit()
    stats['recruitments'] = inserted_recruitments

    inserted_signups = 0
    signup_pairs = [
        (0, "student1"), (0, "student4"),
        (1, "student1"), (1, "student3"),
        (2, "student5"),
    ]
    for rec_idx, student_key in signup_pairs:
        if rec_idx >= len(recruitment_ids):
            continue
        rec_id, club_name = recruitment_ids[rec_idx]
        uid = user_ids.get(student_key, 0)
        profile = STUDENT_PROFILES.get(student_key, {})
        cur.execute(
            '''INSERT INTO recruitment_signups
            (recruitment_id, user_id, student_name, student_class, student_id_num, student_phone, club_name)
            VALUES (?, ?, ?, ?, ?, ?, ?)''',
            (rec_id, uid, profile.get('real_name', student_key), profile.get('class_name', ''),
             profile.get('student_id', ''), profile.get('phone', ''), club_name)
        )
        inserted_signups += 1
    conn.commit()
    stats['recruitment_signups'] = inserted_signups

    inserted_finances = 0
    for fin in FINANCE_RECORDS:
        cur.execute(
            '''INSERT INTO finance_records
            (club_name, type, category, amount, description, record_date, recorder)
            VALUES (?, ?, ?, ?, ?, ?, ?)''',
            (fin['club_name'], fin['type'], fin['category'], fin['amount'],
             fin['description'], fin['record_date'], fin['recorder'])
        )
        inserted_finances += 1
    conn.commit()
    stats['finance_records'] = inserted_finances

    inserted_members = 0
    for member in CLUB_MEMBERS_DATA:
        student_key = None
        for sk, sp in STUDENT_PROFILES.items():
            if sp['real_name'] == member['real_name']:
                student_key = sk
                break
        uid = user_ids.get(student_key, 0)
        username = student_key if student_key else ''
        existing = cur.execute(
            'SELECT id FROM club_members WHERE club_name=? AND real_name=? AND student_id_num=?',
            (member['club_name'], member['real_name'], member['student_id_num'])
        ).fetchone()
        if existing:
            continue
        cur.execute(
            '''INSERT INTO club_members
            (club_name, user_id, username, real_name, student_id_num, class_name, phone, source)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
            (member['club_name'], uid, username, member['real_name'],
             member['student_id_num'], member['class_name'], member['phone'], member['source'])
        )
        inserted_members += 1
    conn.commit()
    stats['club_members'] = inserted_members

    conn.close()

    print("=" * 50)
    print("  模拟数据写入完成")
    print("=" * 50)
    total = 0
    for table, count in stats.items():
        print(f"  {table:30s} +{count}")
        total += count
    print("-" * 50)
    print(f"  {'合计':30s} +{total}")
    print("=" * 50)


if __name__ == '__main__':
    main()
