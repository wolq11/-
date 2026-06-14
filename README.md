# 云智社联 — 高校智慧社团管理平台

> 基于 Flask + SQLite 的全栈高校社团活动管理与数据统计分析系统，覆盖社团全生命周期管理，助力高校社团工作数字化转型。

---

## 目录

- [项目背景](#项目背景)
- [核心亮点](#核心亮点)
- [技术架构](#技术架构)
- [功能全景](#功能全景)
- [项目结构](#项目结构)
- [快速开始](#快速开始)
- [配置说明](#配置说明)
- [数据库设计](#数据库设计)
- [API 接口](#api-接口)
- [部署指南](#部署指南)
- [使用说明](#使用说明)
- [更新日志](#更新日志)
- [许可证](#许可证)

---

## 项目背景

当前高校学生社团管理普遍面临以下痛点：

- **信息孤岛**：社团数据分散在微信群、Excel 表格中，缺乏统一管理入口
- **流程繁琐**：活动计划提交、审批、签到、总结等环节依赖人工传递，效率低下
- **数据盲区**：缺乏系统化的活动数据采集与分析能力，无法量化社团活跃度
- **监管困难**：指导老师与管理员难以及时掌握社团真实运营状况

**云智社联**应运而生，旨在为高校提供一套**轻量、实用、数据驱动**的社团管理解决方案，实现从社团注册、活动开展、签到考勤到数据统计的全流程数字化闭环。

---

## 核心亮点

1. **全流程覆盖**：从社团注册、成员招募、活动策划、签到签退、活动总结到数据统计分析，完整闭环
2. **多角色协作**：管理员、社团负责人、指导老师、学生四级角色体系，权限清晰
3. **三种签到方式**：签到码签到、二维码签到、GPS 定位签到，适应不同场景
4. **智能数据清洗**：内置 DataCleaner 引擎，支持去重、空值处理、规范化
5. **MapReduce 统计**：后端实现 MapReduce 算法对活动数据进行多维聚合分析
6. **智能预警**：自动检测社团活跃度骤降、赋分停滞、审批超时等异常，主动推送预警通知
7. **智能周报**：自动生成社团活动周报，汇总本周活动数据
8. **文件解析**：支持 Word（.doc/.docx）和 PDF 文件自动文本提取与内容校验
9. **响应式设计**：前端适配 PC 端与移动端，移动端支持扫码签到
10. **云存储兼容**：支持本地存储与阿里云 OSS 双模式，灵活切换

---

## 技术架构

### 技术栈

| 层级 | 技术 | 说明 |
|------|------|------|
| **后端框架** | Flask 2.3+ | 轻量级 Python Web 框架 |
| **数据库** | SQLite 3 | 零配置嵌入式数据库，适合中小规模部署 |
| **数据分析** | Pandas 2.0+ | 数据清洗、聚合、统计 |
| **Excel 处理** | openpyxl 3.1+ / xlrd 2.0+ | Excel 导入导出与读写 |
| **Word 处理** | python-docx 1.0+ | Word 文档解析与文本提取 |
| **PDF 处理** | PyMuPDF 1.23+ | PDF 文档文本提取 |
| **云存储** | oss2 2.18+ | 阿里云 OSS 对象存储（可选） |
| **前端** | HTML5 + CSS3 + JavaScript（原生） | 无框架依赖，轻量高效 |
| **图表** | Chart.js | 数据可视化图表 |

### 架构图

```
┌─────────────────────────────────────────────────────┐
│                     前端层 (Frontend)                  │
│  dashboard.html  │  club-tools.html  │  index.html    │
│  checkin.html    │  login.html       │  feedback.html │
│  club-teacher.html│  workload.html   │  stats.html    │
└────────────────────────┬────────────────────────────┘
                         │  RESTful API (JSON)
┌────────────────────────▼────────────────────────────┐
│                    后端层 (Backend)                    │
│                                                       │
│  ┌──────────┐  ┌──────────┐  ┌──────────────────┐  │
│  │ 路由控制器 │  │ 认证中间件 │  │ 数据清洗引擎      │  │
│  │ (Routes)  │  │ (Auth)   │  │ (DataCleaner)    │  │
│  └──────────┘  └──────────┘  └──────────────────┘  │
│  ┌──────────┐  ┌──────────┐  ┌──────────────────┐  │
│  │ MapReduce │  │ 审批代理  │  │ 智能预警引擎      │  │
│  │ 统计引擎   │  │(Approval) │  │ (AlertCenter)    │  │
│  └──────────┘  └──────────┘  └──────────────────┘  │
│  ┌──────────┐  ┌──────────┐  ┌──────────────────┐  │
│  │ 文件解析器 │  │ 通知服务  │  │ 数据洞察代理      │  │
│  │(Parser)   │  │(Notify)  │  │ (DataInsight)     │  │
│  └──────────┘  └──────────┘  └──────────────────┘  │
└────────────────────────┬────────────────────────────┘
                         │
┌────────────────────────▼────────────────────────────┐
│                  数据层 (Data)                         │
│                                                       │
│  ┌──────────────┐  ┌──────────────────────────────┐  │
│  │   SQLite DB   │  │  文件存储 (本地/OSS)           │  │
│  │ club_stats.db │  │  activity_plans/  summaries/  │  │
│  │               │  │  club_uploads/    notices/    │  │
│  │  20+ 数据表   │  │  dept_images/     offcampus/  │  │
│  └──────────────┘  └──────────────────────────────┘  │
└─────────────────────────────────────────────────────┘
```

---

## 功能全景

### 一、多角色权限体系

| 角色 | 核心职责 | 主要功能 |
|------|----------|----------|
| **管理员** | 统筹全局管理 | 社团审批、数据统计、用户管理、预警监控、系统配置 |
| **社团负责人** | 管理单个社团 | 活动发布、成员管理、部门建设、签到管理、材料提交 |
| **指导老师** | 指导多社团 | 活动审核、签到确认、跨社团监督、督学指导 |
| **学生** | 参与社团活动 | 社团报名、活动签到、查看记录、树洞发言 |

### 二、管理员功能模块

#### 首页概览
- 活动概览：全校社团活动总览，实时统计活动数量、参与人数
- 消息通知：系统通知中心，审批提醒、预警通知

#### 社团管理
- **活动概览**：全校社团活动总览仪表盘
- **社团活动列表**：所有已完成活动的详细列表，支持按日期筛选、搜索，支持下载活动计划、查看活动总结、活动照片；支持选为优秀活动、警告活动
- **活动统计分析**：活动数据上传与多维分析，支持 MapReduce 聚合统计
- **指导老师指导情况**：查看各指导老师参与社团活动的签到签退记录
- **社团结构**：查看所有社团的组织结构、部门设置、成员分配，支持导出 Excel（含部门及部门简介）
- **材料管理**：社团提交的各类材料统一管理归档
- **校外活动审批**：校外活动申请审批流程
- **问题反馈**：处理学生提交的问题反馈

#### 活动招募与赋分
- **招募管理**：社团招新报名审批管理
- **联合活动**：跨社团联合活动广场
- **赋分规则**：按星级配置集体和个人赋分上限
- **最终学分**：综合活动参与与工作量，计算最终学分

#### 首页管理
- **首页轮播图**：管理首页展示轮播内容
- **优秀社团活动**：精选优秀活动展示在首页
- **热点资讯**：发布系统公告与热点资讯

#### 其他功能
- **智能周报**：自动生成周度活动数据报告
- **管理员通知**：向指定用户或社团发送通知
- **资料库**：文档分类索引与检索
- **预警中心**：自动检测社团异常（活跃度骤降、赋分停滞、审批超时等）并推送预警
- **树洞管理**：匿名树洞内容审核管理
- **用户管理**：用户账号管理、角色分配

### 三、社团负责人功能模块

#### 社团主页
- **社团简介**：编辑社团基本信息、Logo 上传、星级、指导单位
- **指导老师**：查看指导老师信息
- **部门结构**：创建和管理社团内部部门（部门名称、部门简介、部门图片），构建树形组织结构

#### 活动开展
- 创建签到活动：填写活动名称、地点、时间、内容，上传活动计划（Word/PDF）
- 三种签到方式：
  - **签到码签到**：系统生成 6 位数字签到码，学生输入即可签到
  - **二维码签到**：生成签到二维码，学生扫码签到
  - **GPS 定位签到**：设置签到经纬度，学生在指定位置签到
- 签退管理：支持签到码签退和定位签退
- 活动完成后上传活动照片和活动总结，完结活动
- 查看活动签到记录、统计分析

#### 社团管理
- **报名审批**：审批学生加入社团的申请
- **成员管理**：查看和管理社团成员列表
- **成员分析**：成员数据可视化（学院分布、年级分布等）

#### 社团工具
- **接龙报名**：社团内部活动接龙，支持人数限制
- **活动报名**：社团活动报名，支持名额限制和角色选择
- **投票**：社团内部投票，支持匿名投票、结果可见性控制
- **问卷**：社团内部调查问卷

#### 其他功能
- 工作量申报与审批
- 财务记录管理（收支记录、附件上传）
- 校外活动申请
- 退社申请处理

### 四、指导老师功能模块

- 切换管理多个社团
- 查看社团活动列表与签到记录
- 活动签到签退（老师身份）
- 查看指导老师通知
- 被邀请加入社团
- 个人资料管理

### 五、学生功能模块

- 浏览社团列表，查看社团详情
- 加入社团（提交报名申请）
- 活动签到签退（签到码/二维码/定位）
- 查看个人签到记录
- 树洞匿名发言
- 问题反馈提交
- 查看个人学分与工作量
- 个人资料管理、头像上传

### 六、文件处理能力

- **Word 文档解析**：自动提取 .doc/.docx 文件文本内容，支持字数统计与内容校验
- **PDF 文档解析**：自动提取 PDF 文件文本内容
- **图片上传**：支持头像、Logo、活动照片、部门图片上传
- **文件归档**：活动计划、活动总结、财务附件统一存储管理

---

## 项目结构

```
云智社联/
├── server.py                  # Flask 后端主程序（约 17500 行，含全部路由与业务逻辑）
├── storage.py                 # 文件存储模块（本地存储 / 阿里云 OSS 双模式）
├── init_data.py               # 测试数据初始化脚本（生成 4 个社团 + 成员 + 活动数据）
├── requirements.txt           # Python 依赖清单
├── add_student_union.py       # 学生会数据添加脚本
├── student_accounts.txt       # 学生账号列表
├── stuclub.sh                 # 服务启动脚本
├── README.md                  # 项目说明文档
├── .gitignore                 # Git 忽略配置
├── public/                    # 前端静态资源
│   ├── index.html             # 首页（游客浏览）
│   ├── login.html             # 登录页面
│   ├── dashboard.html         # 管理控制台（管理员/社团负责人）
│   ├── club-teacher.html      # 指导老师工作台
│   ├── club-tools.html        # 社团工具页（接龙、报名、投票、问卷）
│   ├── checkin.html           # 学生签到页面
│   ├── feedback.html          # 问题反馈页面
│   ├── workload.html          # 工作量统计页面
│   ├── stats.html             # 统计展示页面
│   ├── upload.html            # 文件上传页面
│   ├── club-detail.html       # 社团详情页
│   ├── uploads/               # 上传文件目录
│   └── images/                # 静态图片资源
└── data/                      # 数据目录（运行时生成）
    ├── club_stats.db           # SQLite 数据库文件
    ├── activity_plans/         # 活动计划文件
    ├── activity_photos/        # 活动照片
    ├── activity_summaries/     # 活动总结文件
    ├── club_uploads/           # 社团上传文件
    ├── dept_images/            # 部门图片
    ├── enroll_attachments/     # 报名附件
    ├── feedback_files/         # 反馈附件
    ├── finance_attachments/    # 财务附件
    ├── notices/                # 通知附件
    └── offcampus/              # 校外活动申请文件
```

---

## 快速开始

### 环境要求

- Python 3.8+
- pip 包管理器
- SQLite 3（Python 内置）

### 安装步骤

```bash
# 1. 克隆项目
git clone https://github.com/wolq11/-
cd -

# 2. 创建虚拟环境
python -m venv venv

# 3. 激活虚拟环境
# Linux/Mac
source venv/bin/activate
# Windows
venv\Scripts\activate

# 4. 安装依赖
pip install -r requirements.txt

# 5. 初始化测试数据（可选）
python init_data.py

# 6. 启动服务
python server.py
```

### 访问地址

服务启动后，访问以下地址：

| 页面 | URL | 说明 |
|------|-----|------|
| 首页 | http://localhost:5000/ | 访客浏览页面 |
| 登录 | http://localhost:5000/login.html | 用户登录入口 |
| 管理控制台 | http://localhost:5000/dashboard.html | 管理员 / 社团负责人工作台 |
| 指导老师 | http://localhost:5000/club-teacher.html | 指导老师工作台 |
| 签到 | http://localhost:5000/checkin.html | 学生签到入口 |
| 反馈 | http://localhost:5000/feedback.html | 问题反馈入口 |

### 测试账号

运行 `python init_data.py` 后将生成以下测试账号：

| 账号 | 密码 | 角色 | 说明 |
|------|------|------|------|
| 0066 | 0000 | 管理员 | 系统管理员 |
| 张三 | 123456 | 社团负责人 | 羽毛球社负责人 |
| 孙七 | 123456 | 指导老师 | 指导多个社团 |

---

## 配置说明

### 环境变量

创建 `.env` 文件（可选，不创建则使用默认配置）：

```env
# 文件存储类型: local（默认）或 oss
STORAGE_TYPE=local

# 阿里云 OSS 配置（STORAGE_TYPE=oss 时必填）
OSS_ACCESS_KEY_ID=your_access_key
OSS_ACCESS_KEY_SECRET=your_secret
OSS_ENDPOINT=oss-cn-beijing.aliyuncs.com
OSS_BUCKET=your_bucket
OSS_PREFIX=club-stats

# Flask 配置
SECRET_KEY=your_secret_key
DEBUG=False
```

### 文件存储说明

- **本地存储（默认）**：所有上传文件保存在 `data/` 目录下，按类型分子目录
- **OSS 云存储**：配置 OSS 环境变量后，文件自动上传至阿里云 OSS，适合生产环境部署

---

## 数据库设计

### 核心数据表

| 表名 | 说明 | 关键字段 |
|------|------|----------|
| `users` | 用户账号 | id, username, password, role, club_name, real_name, phone, email |
| `user_profiles` | 用户扩展信息 | user_id, class_name, student_id, avatar, introduction |
| `club_profiles` | 社团档案 | club_name, star_rating, category, president, guiding_unit, emblem_url, description |
| `club_members` | 社团成员 | id, club_name, user_id, department, joined_at |
| `club_departments` | 社团部门结构 | id, club_name, dept_name, description, parent_id, image_path |
| `club_cadres` | 社团骨干 | id, club_name, user_id, position, level |
| `teacher_clubs` | 指导老师关联 | id, user_id, club_name |
| `club_teachers` | 社团指导老师 | id, club_name, teacher_name, user_id |
| `checkin_sessions` | 签到会话 | id, club_name, activity_name, checkin_code, status, plan_text, plan_path, summary_text, summary_path, completion_photo, is_completed, warning |
| `checkin_records` | 签到记录 | id, session_id, student_name, student_id, checkin_method, checkin_time |
| `teacher_checkin_checkout` | 老师签到签退 | id, session_id, teacher_user_id, status |
| `club_registrations` | 社团报名 | id, club_name, student_name, status, reviewed_at |
| `club_tools` | 社团工具 | id, club_name, tool_type, title, description, options, participants, results |
| `notifications` | 通知消息 | id, user_id, title, content, type, is_read |
| `feedbacks` | 问题反馈 | id, user_id, club_name, title, body, status, file_path |
| `finance_records` | 财务记录 | id, club_name, type, amount, description, attachment_path |
| `offcampus_requests` | 校外活动申请 | id, club_name, title, description, status, file_path |
| `workload_submissions` | 工作量申报 | id, student_user_id, club_name, description, score, status |
| `scoring_rules` | 赋分规则 | id, star_level, collective_limit, individual_limit |
| `club_overrides` | 社团赋分覆写 | id, club_name, collective_limit, individual_limit |
| `excellent_activities` | 优秀活动 | id, group_id, title, selected_at |
| `notices` | 公告/资讯 | id, title, content, attachment_path |
| `treehole_posts` | 树洞帖子 | id, user_id, content, status, created_at |
| `quit_applications` | 退社申请 | id, user_id, club_name, reason, status |

### 数据库特点

- **SQLite 单文件**：数据库即文件，迁移方便，无需额外安装数据库服务
- **自动建表**：首次启动时自动创建所有数据表，无需手动执行 SQL
- **兼容性迁移**：通过 ALTER TABLE 动态添加新字段，兼容旧版本数据库
- **行级锁**：SQLite 写入时自动加锁，保证数据一致性

---

## API 接口

系统提供 RESTful API，数据格式为 JSON。以下列出主要接口分类：

### 认证相关

| 方法 | 路径 | 描述 | 认证 |
|------|------|------|------|
| POST | `/api/login` | 用户登录 | 否 |
| POST | `/api/logout` | 用户登出 | 是 |
| POST | `/api/register` | 用户注册 | 否 |
| POST | `/api/reset-password` | 重置密码 | 是 |
| GET | `/api/current-user` | 获取当前用户 | 是 |
| GET | `/api/my-profile` | 获取个人资料 | 是 |
| POST | `/api/my-profile` | 更新个人资料 | 是 |
| POST | `/api/upload-avatar` | 上传头像 | 是 |

### 社团管理

| 方法 | 路径 | 描述 | 认证 |
|------|------|------|------|
| GET | `/api/club-structures` | 获取所有社团结构 | 是 |
| GET | `/api/export-club-structures` | 导出社团结构 Excel | 是（管理员） |
| POST | `/api/club-logo` | 上传社团 Logo | 是 |
| GET | `/api/club-departments/<club_name>` | 获取社团部门 | 是 |
| POST | `/api/club-departments` | 创建部门 | 是 |
| PUT | `/api/club-departments/<id>` | 更新部门 | 是 |
| DELETE | `/api/club-departments/<id>` | 删除部门 | 是 |
| POST | `/api/department-image/<id>` | 上传部门图片 | 是 |
| GET | `/api/club-leaders` | 获取社团负责人 | 是 |
| POST | `/api/club-leaders` | 创建社团负责人 | 是 |
| PUT | `/api/club-leaders/<id>` | 更新社团负责人 | 是 |
| DELETE | `/api/club-leaders/<id>` | 删除社团负责人 | 是 |
| POST | `/api/club-leaders/batch-replace` | 批量替换负责人 | 是 |
| GET | `/api/club-cadres` | 获取社团骨干 | 是 |
| POST | `/api/club-cadres` | 添加社团骨干 | 是 |
| DELETE | `/api/club-cadres/<id>` | 删除社团骨干 | 是 |

### 活动签到

| 方法 | 路径 | 描述 | 认证 |
|------|------|------|------|
| GET | `/api/checkin-sessions` | 获取签到会话列表 | 是 |
| POST | `/api/checkin-sessions` | 创建签到会话 | 是 |
| GET | `/api/checkin-sessions/<id>` | 获取单个签到会话 | 是 |
| PUT | `/api/checkin-sessions/<id>` | 更新签到会话（关闭/重开） | 是 |
| DELETE | `/api/checkin-sessions/<id>` | 删除签到会话 | 是 |
| POST | `/api/checkin` | 学生签到（签到码/二维码） | 否 |
| POST | `/api/location-checkin` | GPS 定位签到 | 否 |
| POST | `/api/teacher-checkin` | 指导老师签到 | 是 |
| POST | `/api/upload-activity-plan` | 上传活动计划文件 | 是 |
| POST | `/api/upload-activity-photo` | 上传活动照片 | 是 |
| POST | `/api/upload-activity-summary` | 上传活动总结文件 | 是 |
| GET | `/api/activity-plan-file/<key>` | 下载活动计划文件 | 是 |
| GET | `/api/activity-plan-text/<id>` | 下载活动计划文本 | 是 |
| GET | `/api/all-completed-activities` | 获取所有已完成活动 | 是（管理员） |
| POST | `/api/activity-warning/<id>` | 活动警告 | 是（管理员） |

### 报名与招募

| 方法 | 路径 | 描述 | 认证 |
|------|------|------|------|
| GET | `/api/my-registrations` | 我的报名记录 | 是 |
| GET | `/api/my-club-memberships` | 我的社团成员关系 | 是 |
| POST | `/api/registration-approve` | 审批报名 | 是 |

### 赋分与学分

| 方法 | 路径 | 描述 | 认证 |
|------|------|------|------|
| GET | `/api/scoring-rules` | 获取赋分规则 | 是 |
| POST | `/api/scoring-rules` | 设置赋分规则 | 是（管理员） |
| GET | `/api/final-credits` | 获取最终学分 | 是 |
| POST | `/api/calculate-credits` | 计算学分 | 是（管理员） |

### 社团工具

| 方法 | 路径 | 描述 | 认证 |
|------|------|------|------|
| GET | `/api/club-tools/<club_name>` | 获取社团工具列表 | 是 |
| POST | `/api/club-tools` | 创建工具（接龙/报名/投票/问卷） | 是 |
| PUT | `/api/club-tools/<id>` | 更新工具 | 是 |
| DELETE | `/api/club-tools/<id>` | 删除工具 | 是 |
| POST | `/api/club-tools/<id>/participate` | 参与工具 | 是 |

### 通知与反馈

| 方法 | 路径 | 描述 | 认证 |
|------|------|------|------|
| GET | `/api/notifications` | 获取通知列表 | 是 |
| POST | `/api/notifications/read` | 标记已读 | 是 |
| GET | `/api/nav-badges` | 获取导航红点数 | 是 |
| POST | `/api/submit-feedback` | 提交反馈 | 是 |
| GET | `/api/all-feedbacks` | 获取全部反馈 | 是（管理员） |

### 财务与工作量

| 方法 | 路径 | 描述 | 认证 |
|------|------|------|------|
| GET | `/api/finance-records/<club_name>` | 获取财务记录 | 是 |
| POST | `/api/finance-records` | 创建财务记录 | 是 |
| DELETE | `/api/finance-records/<id>` | 删除财务记录 | 是 |
| GET | `/api/finance-summary/<club_name>` | 财务汇总 | 是 |
| GET | `/api/workload-submissions` | 获取工作量申报 | 是 |
| POST | `/api/workload-submissions` | 提交工作量申报 | 是 |
| POST | `/api/approve-workload` | 审批工作量 | 是 |

### 其他功能

| 方法 | 路径 | 描述 | 认证 |
|------|------|------|------|
| GET | `/api/weekly-report` | 获取智能周报 | 是 |
| POST | `/api/admin-notify` | 发送管理员通知 | 是（管理员） |
| GET | `/api/alert-center` | 获取预警信息 | 是 |
| GET | `/api/treehole-posts` | 获取树洞帖子 | 是 |
| POST | `/api/treehole-posts` | 发布树洞帖子 | 是 |
| DELETE | `/api/treehole-posts/<id>` | 删除树洞帖子 | 是（管理员） |
| GET | `/api/offcampus-requests` | 获取校外活动申请 | 是 |
| POST | `/api/offcampus-requests` | 提交校外活动申请 | 是 |
| PUT | `/api/offcampus-requests/<id>` | 审批校外活动 | 是（管理员） |
| GET | `/api/users` | 获取用户列表 | 是（管理员） |
| POST | `/api/users` | 创建用户 | 是（管理员） |
| GET | `/api/teacher-profile` | 获取指导老师资料 | 是 |
| POST | `/api/teacher-profile` | 更新指导老师资料 | 是 |
| GET | `/api/teacher-guidance` | 获取指导老师指导情况 | 是（管理员） |
| GET | `/api/quit-applications/manage` | 获取退社申请 | 是 |
| POST | `/api/quit-apply` | 提交退社申请 | 是 |
| POST | `/api/quit-applications/<id>/handle` | 处理退社申请 | 是 |

---

## 部署指南

### 开发环境

```bash
# 安装依赖
pip install -r requirements.txt

# 启动开发服务器（Debug 模式，自动重载）
python server.py
```

### 生产环境

```bash
# 安装 gunicorn
pip install gunicorn

# 后台启动
gunicorn --workers 4 --bind 127.0.0.1:5000 --daemon server:app

# 配置 Nginx 反向代理
# 在 /etc/nginx/sites-available/ 中添加配置，指向 127.0.0.1:5000
```

### Docker 部署

```dockerfile
FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p data

EXPOSE 5000

CMD ["gunicorn", "--workers", "4", "--bind", "0.0.0.0:5000", "server:app"]
```

构建与运行：

```bash
docker build -t club-stats .
docker run -d -p 5000:5000 -v $(pwd)/data:/app/data club-stats
```

---

## 使用说明

### 管理员操作流程

1. **登录系统**：访问 `/dashboard.html`，使用管理员账号登录
2. **社团管理**：在左侧菜单"社团"板块中查看活动概览、活动列表、社团结构
3. **审批管理**：处理校外活动审批、问题反馈
4. **数据统计**：在"活动统计分析"模块上传数据、查看分析结果
5. **首页管理**：配置轮播图、优秀活动、热点资讯
6. **预警监控**：在"预警中心"查看系统自动检测的异常警告
7. **用户管理**：在"用户管理"中管理所有用户账号

### 社团负责人操作流程

1. **完善社团主页**：编辑社团简介、上传 Logo、创建部门结构
2. **审批成员**：在"报名审批"中通过或拒绝入社申请
3. **发起活动**：在"活动开展"中创建活动，上传活动计划，选择签到方式
4. **管理签到**：活动进行中监控签到情况，活动结束后上传照片和总结
5. **使用社团工具**：发起接龙、报名、投票、问卷等

### 学生操作流程

1. **浏览社团**：在首页查看社团列表，进入详情页了解社团信息
2. **报名加入**：填写报名信息提交申请，等待审批
3. **参与签到**：活动时使用签到码、扫描二维码或 GPS 定位签到
4. **查看记录**：在个人中心查看签到历史与学分
5. **树洞互动**：在树洞匿名发言交流

---

## 更新日志

### v1.0.0 (2026-06)

**新增功能：**
- 完整的社团管理全流程（注册、成员、活动、签到、总结）
- 三种签到方式（签到码、二维码、GPS 定位）
- 社团部门结构管理（树形结构、部门简介）
- 社团工具模块（接龙、报名、投票、问卷）
- 智能预警中心（社团活跃度、赋分停滞、审批超时等自动检测）
- 智能周报自动生成
- 指导老师工作台（多社团切换、签到签退）
- 树洞匿名发言功能
- 工作量申报与审批
- 财务记录管理
- 校外活动审批流程
- 赋分规则与学分计算
- 活动材料管理（活动计划、总结、照片）
- 文件上传与解析（Word/PDF 文本提取）
- 数据导出（Excel 格式）
- 数据清洗引擎（去重、空值处理、规范化）
- MapReduce 统计引擎
- 响应式前端设计（PC + 移动端）
- 本地存储与阿里云 OSS 双模式支持

---

## 许可证

MIT License

---

## 联系方式

- GitHub: [https://github.com/wolq11/-](https://github.com/wolq11/-)