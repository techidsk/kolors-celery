# Kolors Celery 任务处理系统

这是一个基于 Flask 和 Celery 的分布式任务处理系统，使用 Redis 作为消息代理和结果后端。该系统提供了一个 RESTful API 接口，用于创建和管理异步任务。

## 主要功能

- 异步任务处理
- RESTful API 接口
- 任务状态查询
- 定时任务调度
- 健康检查接口
- Redis 持久化存储
- 完整的日志系统

## 技术栈

- Python
- Flask (Web 框架)
- Celery (分布式任务队列)
- Redis (消息代理和结果后端)
- Loguru (日志管理)
- Gunicorn (WSGI HTTP 服务器)

## 项目结构

```
kolors-celery/
├── api.py              # Flask API 服务
├── tasks.py            # Celery 任务定义
├── jobs.py             # 定时任务配置
├── celery_config.py    # Celery 配置
├── local_worker.py     # 本地工作进程
├── requirements.txt    # 项目依赖
└── .env               # 环境变量配置
```

## 安装

1. 克隆项目并安装依赖：

```bash
git clone <repository-url>
cd kolors-celery
pip install -r requirements.txt
```

2. 配置环境变量：

创建 `.env` 文件并设置以下变量：
```
REDIS_PASSWORD=your_redis_password
REDIS_HOST=your_redis_host
```

## API 接口

### 创建任务
- **POST** `/create_task`
- 请求体: `{"payload": "your_data"}`
- 返回: `{"task_id": "task_id", "status": "Task enqueued"}`

### 获取任务结果
- **GET** `/get_result/<task_id>`
- 返回: 任务状态和结果

### 健康检查
- **GET** `/health`
- 返回: 系统各组件的健康状态

## 部署

系统支持通过 systemd 或 supervisor 进行服务管理：

1. API 服务部署
2. Celery 工作进程部署
3. 后台任务调度

## 日志

系统使用 Loguru 进行日志管理，日志文件位于 `logs/api.log`，支持：
- 自动日志轮转（10MB）
- 日志保留策略（1周）
- ZIP 压缩
- 详细的错误追踪

## 注意事项

1. 确保 Redis 服务器正在运行
2. 正确配置环境变量
3. 确保有适当的系统权限运行服务

## 许可证

[MIT License](LICENSE)
