import sqlite3
conn = sqlite3.connect('data/club_stats.db')

# 1. Add club profile for 广播台
try:
    conn.execute("INSERT INTO club_profiles (club_name, description, star_rating, show_star, president, category, guiding_unit, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now','localtime'), datetime('now','localtime'))",
                 ('广播台', '广播台是全校师生获取校园资讯、感受文化氛围的重要窗口，是一支以传递声音、传播资讯、传承文化为使命的校园媒体组织。作为校园文化建设的主力军，我们不仅是校园新闻的播报者，更是青春风采的展示者与校园声音的传递者。在这里，文字与声音交融，时效与品质并重，共同编织着最动听的校园广播篇章。', 5, 1, 'zhaomin_pres', '媒体类', '团委'))
    print('Added 广播台 to club_profiles')
except Exception as e:
    print(f'Error adding club profile: {e}')

# 2. Add 3 departments
depts = [
    ('广播台', '新闻部', '新闻部是广播台中敏锐捕捉资讯、精准传递声音的核心部门，是一支以新闻敏感、严谨求实、追求时效为工作准则的专业型团队。作为校园新闻播报的主力军，我们不仅是校园动态的观察者，更是新闻价值的发现者与资讯传播的把关人。在这里，敏锐与严谨交织，速度与准确并重，共同构筑着校园新闻播报的第一道防线。\n我们的核心职责聚焦校园新闻的全流程管理，从校园新闻的敏锐采集与深入采访，到新闻稿件的精心撰写与严格审核；从新闻内容的准确把关到广播新闻的及时播报。无论是学校重大活动的权威发布，还是校园日常生活的温情记录，新闻部成员始终以高度的政治敏感性和专业的新闻素养，确保每一条新闻都真实准确、每一条资讯都及时有效，为全校师生提供最有价值的校园资讯服务。'),
    ('广播台', '播音部', '播音部是广播台中用声音传递温度、以表达打动人心的专业部门，是一支以声音优美、表达精准、富有感染力为工作标准的表演型团队。作为校园广播节目的直接呈现者，我们不仅是稿件的朗读者，更是情感的传递者与校园氛围的营造者。在这里，声音与情感交融，专业与魅力同行，共同演绎着最动听的校园广播之声。\n我们的核心职责聚焦广播节目的播音主持工作，下设四个专业小组协同运转：播音一组负责早间新闻播报，以清晰准确的声音开启校园新的一天；播音二组负责午间节目主持，以轻松活泼的风格陪伴师生午间时光；播音三组负责晚间音乐节目，以优美动人的旋律营造温馨氛围；播音四组负责专题节目录制，以深度专业的内容展现校园风采。同时，播音部统筹安排各组播音任务，定期开展播音技巧培训，保障每一档广播节目都达到专业水准。'),
    ('广播台', '采编部', '采编部是广播台中策划创意、创作内容的智慧中枢，是一支以创意丰富、文笔优美、善于挖掘为工作特色的创作型团队。作为广播节目内容的策划者与创作者，我们不仅是素材的收集者，更是故事的讲述者与校园文化的深度挖掘者。在这里，创意与文字碰撞，策划与执行并进，共同打造着最富内涵的广播节目内容。\n我们的核心职责聚焦广播节目的策划与内容创作，从节目选题的创意策划到节目脚本的精心撰写；从校园人物与事件的深度采访到广播素材的编辑整理；从新媒体平台内容的运营维护到线上线下活动的联动推广。无论是人物专访的温情笔触，还是校园事件的客观记录，采编部成员始终以敏锐的洞察力和扎实的文字功底，为每一档广播节目注入灵魂与温度。'),
]

for club, dept, desc in depts:
    try:
        conn.execute("INSERT INTO club_departments (club_name, dept_name, description, created_at) VALUES (?, ?, ?, datetime('now','localtime'))",
                     (club, dept, desc))
        print(f'Added: {club} -> {dept} ({len(desc)}字)')
    except Exception as e:
        print(f'Error adding {dept}: {e}')

# 3. Add teacher
try:
    conn.execute("INSERT INTO club_teachers (club_name, teacher_name, introduction, created_at) VALUES (?, ?, ?, datetime('now','localtime'))",
                 ('广播台', '吴九', '广播台指导老师，负责社团整体指导与管理工作。'))
    print('Added teacher: 吴九 -> 广播台')
except Exception as e:
    print(f'Error adding teacher: {e}')

conn.commit()
conn.close()
print('Done!')
