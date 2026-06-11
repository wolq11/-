# 云智社联 - 高校智慧社团活动综合管理系统

> 面向高校社团的全流程智慧管理平台，深度融合 **AI 智能助手**、**自动化 Agent** 与 **多角色协同**，实现从活动策划到学分认定的全链路数字化闭环。

---

## 🌟 创新亮点

### 🤖 AI 智能助手体系

| 功能 | 描述 | 核心能力 |
|------|------|----------|
| **AI 聊天助手** | 全站浮动聊天入口，支持上下文感知 | 审批查询、文件搜索、预警检查、数据报告 |
| **AI 海报生成** | 智能生成社团招募宣传海报 | 一键出图，支持自定义主题与风格 |
| **AI 活动策划** | 根据历史数据智能生成活动方案 | 时间规划、流程设计、资源预估 |
| **AI 公告润色** | 自动优化通知公告文案 | 语法检查、语气调整、格式规范 |
| **AI 招募优化** | 智能优化社团招新文案 | 关键词优化、吸引力提升 |
| **AI 活动总结** | 根据活动数据自动生成总结报告 | 数据统计、亮点提炼、改进建议 |
| **AI 学期计划** | 智能生成学期工作计划 | 目标设定、进度安排、资源规划 |
| **AI 照片分析** | 自动分析活动照片内容 | 场景识别、人物计数、质量评估 |

### 🧠 智能 Agent 自动化系统

- **文档索引 Agent**：自动索引上传文档，支持智能全文搜索
- **数据洞察 Agent**：定期自动生成数据分析报告
- **审批辅助 Agent**：智能分析审批材料，提供审核建议
- **预警通知 Agent**：实时监控异常数据，自动发送预警提醒

### 🎯 智能推荐引擎

基于 **17 种技能关键词库** + 多维度匹配算法：
- 社团名称匹配
- 活动描述匹配
- 分类标签匹配
- 部门需求匹配
- 历史活动偏好

---

## 📋 实用功能

### 👥 四角色权限体系

| 角色 | 核心功能 | 专属特性 |
|------|----------|----------|
| **管理员** | 全局管理、审批决策、数据统计 | 粉色主题、预警中心、系统配置 |
| **社团负责人** | 社团运营、活动开展、成员管理 | 紫色主题、AI工具集、财务登记 |
| **指导老师** | 赋分审核、签到签退、社团指导 | 金色主题、GPS定位签到 |
| **学生** | 活动报名、签到打卡、社团浏览 | 绿色主题、AI推荐、树洞社区 |

### 🔄 全流程活动管理闭环

```
活动策划 → 发布报名 → 签到考勤 → 材料上传 → 审批流转 → 赋分计算 → 学分认定
```

### ✅ 核心功能模块

- **签到考勤**：签到码输入 + 摄像头扫码双模式，支持 GPS 定位
- **三级赋分审核**：社团提交 → 指导老师审核 → 团委审核
- **财务精细化管理**：收支记录、分类统计、附件上传、权限授权
- **协助工具箱**：投票（单选/多选/匿名/限员）、问卷、报名表
- **校外活动审批**：模板填充、规范管理、风险评估
- **树洞匿名社区**：公开/社团内部两种范围，保护隐私
- **联合活动平台**：跨社团合作邀请、留言互动、合作确认

---

## 💻 技术实现

### 技术栈

| 层级 | 技术 | 版本 |
|------|------|------|
| 后端框架 | Python Flask | 3.1+ |
| 数据库 | SQLite | 3.x |
| 数据处理 | Pandas | 3.0+ |
| 文档解析 | python-docx / PyMuPDF | 1.2+ / 1.27+ |
| 文件存储 | 本地 / 阿里云 OSS | - |
| AI 集成 | 通义千问 API | - |
| 前端 | 原生 HTML/CSS/JS | ES6+ |
| 图表 | Chart.js | 4.x |
| 二维码 | qrcodejs2 / jsQR | - |

### 核心技术亮点

#### 🔧 数据引擎

- **DataCleaner**：自实现数据清洗模块（去重/空值处理/规范化）
- **StatsService**：描述性统计分析引擎
- **LRU Cache**：带 TTL 的内存缓存机制

#### 🔐 安全机制

- 密码加密存储（SHA256）
- 会话管理与超时控制
- 角色权限校验
- SQL 注入防护

#### 📦 文件存储

- 本地存储与阿里云 OSS 双模式无缝切换
- 自动迁移旧路径
- 文件大小与类型校验

### 项目结构

```
/opt/liqi/-/
├── server.py              # Flask 主服务（315+ API）
├── storage.py             # 文件存储模块
├── requirements.txt       # Python 依赖
├── data/                  # 数据目录
│   ├── club_stats.db      # SQLite 数据库（38+ 业务表）
│   ├── activity_photos/   # 活动照片
│   ├── activity_plans/    # 活动计划
│   ├── activity_summaries/# 活动总结
│   ├── club_uploads/      # 社团上传文件
│   └── offcampus/         # 校外活动材料
└── public/                # 前端页面（11个页面）
    ├── login.html         # 登录/注册
    ├── dashboard.html     # 管理员/学生主界面
    ├── club-tools.html    # 社团负责人管理
    ├── club-teacher.html  # 指导老师界面
    ├── checkin.html       # 签到页面
    └── ai-chat.js         # AI 聊天组件
```

---

## 🎨 用户体验

### 🎭 角色主题差异化

| 角色 | 主题色 | 视觉标识 |
|------|--------|----------|
| 管理员 | 粉色 | 🌸 |
| 社团负责人 | 紫色 | 🔮 |
| 指导老师 | 金色 | 🌟 |
| 学生 | 绿色 | 💚 |

### 📱 移动端优先设计

- 完整的移动端适配（768px/480px 断点）
- 表格横向滑动支持
- 触控友好的按钮尺寸
- 响应式布局自动调整

### ⚡ 交互体验

- **一键操作**：签到码输入 + 摄像头扫码双模式
- **实时反馈**：Toast 提示、加载动画、状态标签
- **智能引导**：AI 助手根据上下文推荐操作
- **数据可视化**：图表展示活动统计、成员分布、财务趋势

---

## 🚀 快速开始

### 环境要求

```bash
Python 3.8+
pip 20.0+
```

### 安装步骤

```bash
# 克隆项目
git clone https://github.com/wolq11/-.git
cd -

# 创建虚拟环境
python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows

# 安装依赖
pip install -r requirements.txt

# 启动服务
python server.py
```

### 启动选项

```bash
# HTTP 模式（默认）
python server.py

# HTTPS 模式（自动生成证书）
python server.py --ssl
```

服务默认运行在 `http://localhost:5000`

### 配置说明

创建 `.env` 文件配置环境变量：

```env
# SMTP 邮件配置（可选）
SMTP_HOST=smtp.example.com
SMTP_PORT=465
SMTP_USER=your@email.com
SMTP_PASS=your_password

# AI API 密钥（可选）
QWEN_API_KEY=your_qwen_api_key
ZHIPU_API_KEY=your_zhipu_api_key
```

---

## 📊 数据库架构

系统包含 **38+ 张业务表**，核心实体关系：

```
用户体系：users → user_profiles → club_members → club_cadres
社团体系：club_profiles → club_departments → club_teachers
活动体系：checkin_sessions → checkin_records → activity_records
赋分体系：scoring_rules → scoring_submissions → final_credits
财务体系：finance_records → finance_managers
AI 体系：ai_chat_history → doc_index
```

---

## 📈 部署方案

### 生产环境部署

```bash
# 安装 Gunicorn
pip install gunicorn

# 启动生产服务
gunicorn --workers 4 --bind 127.0.0.1:5000 server:app
```

### Nginx 配置示例

```nginx
server {
    server_name your-domain.com;

    location / {
        proxy_pass http://127.0.0.1:5000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }

    listen 443 ssl;
    ssl_certificate /path/to/cert.pem;
    ssl_certificate_key /path/to/key.pem;
}
```

---

## 📝 License

MIT License

---

## 🤝 贡献

欢迎提交 Issue 和 Pull Request！

---

**项目状态**: ✅ 生产就绪 | **API 数量**: 315+ | **数据库表**: 38+ | **前端页面**: 11个