# 社团活动统计分析系统

基于 Flask + SQLite 的高校社团活动管理与统计分析平台，提供完整的社团管理、活动签到、数据统计等功能。

## 🏗️ 技术栈

| 分类 | 技术 | 版本 |
|------|------|------|
| 后端框架 | Flask | 2.3+ |
| 数据库 | SQLite | 3.x |
| 数据分析 | Pandas | 2.0+ |
| Excel处理 | openpyxl / xlrd | 3.1+ / 2.0+ |
| Word处理 | python-docx | 1.0+ |
| PDF处理 | PyMuPDF | 1.23+ |
| 云存储 | Aliyun OSS | 2.18+ |
| 前端 | HTML5 + JavaScript + CSS3 | - |

## ✨ 功能特性

### 🏢 社团管理

- **社团注册与审批**：完整的社团申请流程，支持审批通过/拒绝
- **社团信息管理**：社团简介、logo上传、成员管理
- **社团评分系统**：基于活动参与、签到数据自动计算评分
- **社团排名**：按评分进行排名展示
- **部门管理**：社团内部部门结构管理

### 📋 活动管理

- **活动计划提交**：社团负责人提交活动计划，包含活动方案、时间、地点
- **活动审核**：指导老师和管理员审核活动计划
- **活动签到系统**：支持三种签到方式
  - 🔢 **签到码签到**：输入6位数字签到码
  - 📷 **二维码签到**：扫描二维码进行签到
  - 📍 **定位签到**：基于地理位置的签到验证
- **活动总结**：活动结束后提交总结报告
- **材料归档**：活动相关材料的上传和管理

### 📊 数据统计

- **活动参与统计**：统计各社团活动参与人数
- **签到数据分析**：分析签到率、签到时间分布
- **社团评分计算**：多维度评分算法
- **可视化报表**：图表展示统计数据
- **导出功能**：支持导出Excel、PDF格式报表

### 👥 用户角色

| 角色 | 权限描述 |
|------|----------|
| **管理员** | 系统管理、用户管理、审批、数据查看、系统设置 |
| **社团负责人** | 社团管理、活动发起、签到管理、成员管理 |
| **指导老师** | 活动监督、签到确认、活动审核 |
| **学生** | 活动报名、签到参与、查看社团信息 |

### 🔔 通知系统

- **活动通知**：活动发起、签到开始等通知
- **审批通知**：申请审批结果通知
- **系统通知**：系统更新、重要公告

### 📱 移动端支持

- 响应式设计，适配手机和平板
- 移动端文件上传（支持图片、PDF、Word）
- 扫码签到功能

## 🚀 快速开始

### 环境要求

- Python 3.8+
- pip 包管理器
- SQLite 3（已内置）

### 安装步骤

```bash
# 克隆项目
git clone https://github.com/wolq11/-
cd -

# 创建虚拟环境
python -m venv venv

# 激活虚拟环境
# Windows
venv\Scripts\activate
# Linux/Mac
source venv/bin/activate

# 安装依赖
pip install -r requirements.txt

# 初始化数据库（可选）
python seed_data.py
```

### 配置说明

创建 `.env` 文件（可选）：

```env
# 阿里云OSS配置（可选）
OSS_ACCESS_KEY_ID=your_access_key
OSS_ACCESS_KEY_SECRET=your_secret
OSS_ENDPOINT=oss-cn-beijing.aliyuncs.com
OSS_BUCKET=your_bucket

# 服务配置
SECRET_KEY=your_secret_key
DEBUG=False
```

### 启动服务

```bash
# 开发模式（自动重载）
python server.py

# 或使用 gunicorn 生产模式
gunicorn --workers 4 --bind 0.0.0.0:5000 server:app

# 使用启动脚本
bash stuclub.sh
```

### 访问地址

| 页面 | URL | 说明 |
|------|-----|------|
| 主控制面板 | http://localhost:5000/dashboard.html | 用户登录后首页 |
| 签到页面 | http://localhost:5000/checkin.html | 学生签到入口 |
| 反馈页面 | http://localhost:5000/feedback.html | 问题反馈页面 |
| API 根路径 | http://localhost:5000/api | RESTful API |

## 📁 项目结构

```
├── data/                    # 数据目录（运行时生成）
│   ├── club_stats.db        # 主数据库文件
│   ├── activity_plans/      # 活动计划文件
│   ├── club_uploads/        # 社团上传文件
│   ├── dept_images/         # 部门图片
│   ├── enroll_attachments/  # 报名附件
│   ├── notices/             # 通知文件
│   └── offcampus/           # 校外活动申请文件
├── public/                  # 前端静态资源
│   ├── dashboard.html       # 主控制面板
│   ├── checkin.html         # 签到页面
│   ├── feedback.html        # 反馈页面
│   ├── login.html           # 登录页面
│   ├── index.html           # 首页
│   ├── upload.html          # 文件上传页面
│   ├── stats.html           # 统计页面
│   ├── club-detail.html     # 社团详情
│   ├── club-teacher.html    # 社团指导老师管理
│   ├── club-tools.html      # 社团工具页面
│   ├── workload.html        # 工作量统计
│   ├── uploads/             # 上传文件目录
│   └── images/              # 静态图片资源
├── server.py                # 后端主服务（Flask应用）
├── storage.py               # 云存储模块（OSS封装）
├── requirements.txt         # Python依赖清单
├── seed_data.py             # 数据库初始化脚本
├── stuclub.sh               # 启动脚本
├── .gitignore               # Git忽略配置
└── README.md                # 项目说明文档
```

## 🔌 API 接口

### 认证相关

| 方法 | 路径 | 描述 | 需要登录 |
|------|------|------|----------|
| POST | `/api/login` | 用户登录 | 否 |
| POST | `/api/logout` | 用户登出 | 是 |
| GET | `/api/current-user` | 获取当前用户信息 | 是 |
| GET | `/api/my-profile` | 获取用户详细资料 | 是 |

### 签到相关

| 方法 | 路径 | 描述 | 需要登录 |
|------|------|------|----------|
| POST | `/api/checkin` | 学生签到（签到码/二维码） | 否 |
| POST | `/api/location-checkin` | 定位签到 | 否 |
| GET | `/api/checkin-sessions` | 获取签到会话列表 | 是 |
| POST | `/api/checkin-sessions` | 创建签到会话 | 是 |
| GET | `/api/checkin-sessions/<id>` | 获取单个签到会话 | 是 |
| PUT | `/api/checkin-sessions/<id>` | 更新签到会话 | 是 |
| DELETE | `/api/checkin-sessions/<id>` | 删除签到会话 | 是 |
| GET | `/api/checkin-records/<sid>` | 获取签到记录 | 是 |

### 反馈相关

| 方法 | 路径 | 描述 | 需要登录 |
|------|------|------|----------|
| POST | `/api/submit-feedback` | 提交反馈 | 是 |
| GET | `/api/my-feedbacks` | 获取我的反馈 | 是 |
| GET | `/api/all-feedbacks` | 获取所有反馈（管理员） | 是（管理员） |
| GET | `/api/feedback-file/<id>` | 获取反馈文件 | 是 |
| GET | `/api/feedback-file-by-key/<key>` | 通过key获取文件 | 是 |

### 社团相关

| 方法 | 路径 | 描述 | 需要登录 |
|------|------|------|----------|
| GET | `/api/clubs` | 获取社团列表 | 是 |
| GET | `/api/clubs/<name>` | 获取社团详情 | 是 |
| POST | `/api/clubs` | 创建社团 | 是（管理员） |
| PUT | `/api/clubs/<name>` | 更新社团信息 | 是 |
| GET | `/api/club-members/<name>` | 获取社团成员 | 是 |

### 活动相关

| 方法 | 路径 | 描述 | 需要登录 |
|------|------|------|----------|
| POST | `/api/enroll` | 活动报名 | 是 |
| GET | `/api/enroll-list` | 获取报名列表 | 是 |
| POST | `/api/approve-enroll` | 审批报名 | 是 |

### 统计相关

| 方法 | 路径 | 描述 | 需要登录 |
|------|------|------|----------|
| GET | `/api/checkin-stats/<club>` | 获取签到统计 | 是 |
| GET | `/api/club-score/<club>` | 获取社团评分 | 是 |
| GET | `/api/nav-badges` | 获取导航红点数量 | 是 |

## 🗄️ 数据库结构

### 主要数据表

| 表名 | 说明 | 主要字段 |
|------|------|----------|
| `users` | 用户信息 | id, username, password, role, club_name |
| `club_members` | 社团成员 | id, club_name, user_id, role |
| `checkin_sessions` | 签到会话 | id, club_name, activity_name, checkin_code, status |
| `checkin_records` | 签到记录 | id, session_id, student_name, student_id, checkin_method |
| `feedbacks` | 反馈信息 | id, user_id, type, title, content, status |
| `notifications` | 通知消息 | id, user_id, title, content, type, is_read |
| `teacher_clubs` | 指导老师关联 | id, user_id, club_name |

## 📦 部署指南

### 开发环境

```bash
# 安装依赖
pip install -r requirements.txt

# 启动开发服务器
python server.py
```

### 生产环境

```bash
# 安装 gunicorn
pip install gunicorn

# 启动生产服务器
gunicorn --workers 4 --bind 127.0.0.1:5000 --daemon server:app

# 配置 Nginx 反向代理
# 在 /etc/nginx/sites-available/stuclub.top 添加配置
```

### Docker 部署（可选）

```dockerfile
FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 5000

CMD ["gunicorn", "--workers", "4", "--bind", "0.0.0.0:5000", "server:app"]
```

## 📝 使用说明

### 管理员操作

1. **登录系统**：访问 `/dashboard.html`，使用管理员账号登录
2. **管理社团**：在左侧菜单选择"社团管理"
3. **审批申请**：查看待审批的社团注册、活动申请
4. **查看统计**：在"统计分析"页面查看数据报表

### 社团负责人操作

1. **发起活动**：点击"活动管理" -> "发起活动"
2. **创建签到**：在活动详情页点击"发起签到"
3. **查看签到记录**：在签到会话列表查看签到情况
4. **提交反馈**：如有问题可通过"问题反馈"提交

### 学生操作

1. **活动报名**：在社团详情页报名感兴趣的活动
2. **参与签到**：使用签到码或扫描二维码进行签到
3. **查看记录**：在个人中心查看签到历史

## 🔧 配置说明

### 环境变量

| 变量名 | 说明 | 默认值 |
|--------|------|--------|
| `DEBUG` | 调试模式 | False |
| `SECRET_KEY` | 会话密钥 | 自动生成 |
| `OSS_ACCESS_KEY_ID` | OSS AccessKey | 空 |
| `OSS_ACCESS_KEY_SECRET` | OSS Secret | 空 |
| `OSS_ENDPOINT` | OSS 端点 | 空 |
| `OSS_BUCKET` | OSS Bucket | 空 |

### 文件存储

- 默认使用本地存储，文件保存在 `data/` 目录
- 配置 OSS 后可上传至阿里云对象存储

## 📝 更新日志

### v1.0.0 (2026-06-11)

**新增功能：**
- ✅ 多文件上传功能（支持图片、PDF、Word）
- ✅ 移动端文件上传优化
- ✅ 签到历史记录查看

**修复问题：**
- ✅ 修复签到时间时区问题（UTC与北京时间转换）
- ✅ 修复反馈图片查看问题（兼容新旧数据格式）
- ✅ 修复管理员通知红点计数错误

**优化改进：**
- ✅ 代码结构优化
- ✅ 性能优化

## 📄 许可证

MIT License

## 🤝 贡献

欢迎提交 Issue 和 Pull Request！

### 贡献流程

1. Fork 本仓库
2. 创建功能分支：`git checkout -b feature/your-feature`
3. 提交更改：`git commit -m "Add your feature"`
4. 推送到分支：`git push origin feature/your-feature`
5. 创建 Pull Request

### 代码规范

- Python 代码遵循 PEP 8 规范
- 前端代码使用统一的缩进风格
- 提交信息使用英文描述

## 📧 联系方式

如有问题或建议，请通过以下方式联系：

- GitHub Issues: https://github.com/wolq11/-/issues