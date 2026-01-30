import os
import threading

from celery import Celery
from celery.exceptions import TimeoutError as CeleryTimeoutError
from dotenv import load_dotenv
from flask import Flask, jsonify, request
from loguru import logger
from redis import Redis

from jobs import schedule_task
from tasks import process_task

load_dotenv()

# 从环境变量获取 Redis 密码
REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD")
REDIS_HOST = os.environ.get("REDIS_HOST")
redis_client = Redis(host=REDIS_HOST, port=6379, db=0, password=REDIS_PASSWORD)

celery_app = Celery(
    "psy_api",
    broker=f"redis://:{REDIS_PASSWORD}@{REDIS_HOST}:6379/0",
    backend=f"redis://:{REDIS_PASSWORD}@{REDIS_HOST}:6379/0",
)

app = Flask(__name__)


# 设置日志文件路径
log_file_path = os.path.join(os.path.dirname(__file__), "logs", "api.log")

# 确保日志目录存在
os.makedirs(os.path.dirname(log_file_path), exist_ok=True)

# 配置 loguru logger
logger.add(
    log_file_path,
    rotation="10 MB",  # 当日志文件达到10MB时轮转
    retention="1 week",  # 保留最近一周的日志
    compression="zip",  # 压缩旧的日志文件
    backtrace=True,
    diagnose=True,
    level="INFO",  # 设置日志级别
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",  # 自定义日志格式
)

# 在应用启动时记录一条日志
logger.info("API application started")


# 创建一个新的线程来运行 schedule_task
def run_schedule_task():
    schedule_task()


# 启动 schedule_task 的线程
schedule_thread = threading.Thread(target=run_schedule_task)
schedule_thread.daemon = True
schedule_thread.start()


@app.route("/create_task", methods=["POST"])
def create_task():
    data = request.json
    task = process_task.delay(data["payload"])
    logger.info(f"Task created with ID: {task.id}")
    return jsonify({"task_id": task.id, "status": "Task enqueued"}), 202


@app.route("/get_result/<task_id>", methods=["GET"])
def get_result(task_id):
    """直接查询任务结果，不创建新任务"""
    try:
        logger.info(f"Getting result for task ID: {task_id}")
        
        # 直接用 AsyncResult 查询，不需要创建新的 Celery 任务
        result = celery_app.AsyncResult(task_id)
        
        if result.state == "PENDING":
            # 任务还在队列中等待
            return jsonify({"status": "pending", "task_id": task_id}), 202
        
        elif result.state == "STARTED":
            # 任务正在执行中
            return jsonify({"status": "processing", "task_id": task_id}), 202
        
        elif result.state == "SUCCESS":
            # 任务完成
            return jsonify({"status": "completed", "result": result.result}), 200
        
        elif result.state == "FAILURE":
            # 任务失败
            logger.error(f"Task {task_id} failed: {result.result}")
            return jsonify({
                "status": "failed",
                "task_id": task_id,
                "error": str(result.result)
            }), 500
        
        else:
            # 其他状态（RETRY, REVOKED 等）
            return jsonify({
                "status": result.state.lower(),
                "task_id": task_id
            }), 202

    except Exception as e:
        logger.error(f"Unexpected error in get_result for task {task_id}: {str(e)}")
        return (
            jsonify({"status": "error", "message": "An unexpected error occurred"}),
            500,
        )


@app.route("/hi", methods=["GET"])
def say_hello():
    logger.info("Received request to /hi endpoint")
    return jsonify({"message": "Hello World"}), 200


@app.route("/health", methods=["GET"])
def health_check():
    try:
        # 检查 Redis 连接
        redis_client.ping()

        # 检查 Celery 工作状态
        i = celery_app.control.inspect()
        active = i.active()

        return (
            jsonify(
                {
                    "status": "healthy",
                    "redis": "connected",
                    "celery_workers": active is not None,
                    "active_tasks": active,
                }
            ),
            200,
        )
    except Exception as e:
        return jsonify({"status": "unhealthy", "error": str(e)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
