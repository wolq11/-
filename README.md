# 社团活动统计分析系统

基于 Flask + SQLite 的高校社团活动管理与统计分析平台，提供完整的社团管理、活动签到、数据统计等功能。

## 🏗️ 技术栈

- **后端**: Flask 2.3+
- **数据库**: SQLite 3
- **数据分析**: Pandas
- **文件处理**: openpyxl, xlrd, python-docx, PyMuPDF
- **云存储**: Aliyun OSS
- **前端**: HTML5 + JavaScript + CSS3

## ✨ 功能特性

### 🏢 社团管理
- 社团注册与审批流程
- 社团信息管理（简介、logo、成员）
- 社团评分与排名系统

### 📋 活动管理
- 活动计划提交与审核
- 活动签到系统（二维码/签到码/定位）
- 活动总结与材料归档

### 📊 数据统计
- 活动参与统计
- 签到数据分析
- 社团评分计算
- 可视化报表生成

### 👥 用户角色
- **管理员**: 系统管理、审批、数据查看
- **社团负责人**: 社团管理、活动发起、签到管理
- **指导老师**: 活动监督、签到确认
- **学生**: 活动报名、签到参与

## 🚀 快速开始

### 环境要求
- Python 3.8+
- pip 包管理器

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
```

### 启动服务

```bash
# 开发模式
python server.py

# 或使用 gunicorn 生产模式
gunicorn --workers 4 --bind 0.0.0.0:5000 server:app
```

### 访问地址
- 前端页面: http://localhost:5000/dashboard.html
- API 文档: http://localhost:5000/api

## 📁 项目结构

```
├── data/                    # 数据库文件目录
├── public/                  # 前端静态资源
│   ├── dashboard.html       # 主控制面板
│   ├── checkin.html         # 签到页面
│   └── feedback.html        # 反馈页面
├── server.py                # 后端主服务
├── storage.py               # 云存储模块
├── requirements.txt         # 依赖清单
└── seed_data.py             # 初始化数据脚本
```

## 🔌 API 接口

### 签到相关
- `POST /api/checkin` - 学生签到
- `POST /api/location-checkin` - 定位签到
- `GET /api/checkin-sessions` - 获取签到会话列表
- `POST /api/checkin-sessions` - 创建签到会话

### 反馈相关
- `POST /api/submit-feedback` - 提交反馈
- `GET /api/my-feedbacks` - 获取我的反馈
- `GET /api/all-feedbacks` - 获取所有反馈（管理员）

### 文件服务
- `GET /api/feedback-file/<id>` - 获取反馈文件
- `GET /api/feedback-file-by-key/<key>` - 通过key获取文件

## 📝 更新日志

### v1.0.0 (2026-06-11)
- ✅ 修复签到时间时区问题（UTC与北京时间转换）
- ✅ 添加多文件上传功能到反馈系统
- ✅ 修复反馈图片查看问题
- ✅ 修复管理员通知红点计数错误
- ✅ 支持手机端文件上传（PDF、Word、图片）

## 📄 许可证

MIT License

## 🤝 贡献

欢迎提交 Issue 和 Pull Request！