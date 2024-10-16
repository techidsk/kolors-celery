import os

from flask import Flask, jsonify, request
from loguru import logger

from tasks import get_task_result, process_task

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
    level="INFO",  # 设置日志级别
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",  # 自定义日志格式
)

# 在应用启动时记录一条日志
logger.info("API application started")


@app.route("/create_task", methods=["POST"])
def create_task():
    data = request.json
    task = process_task.delay(data["payload"])
    logger.info(f"Task created with ID: {task.id}")
    return jsonify({"task_id": task.id, "status": "Task enqueued"}), 202


@app.route("/get_result/<task_id>", methods=["GET"])
def get_result(task_id):
    logger.info(f"Getting result for task ID: {task_id}")
    check_task = get_task_result.apply_async(args=[task_id], expires=60)
    
    try:
        response = check_task.get(timeout=2)
        if response == "Task is still processing...":
            logger.info(f"Task {task_id} is still processing...")
            return jsonify({"status": "pending", "task_id": task_id}), 202
        elif response == "Task failed":
            logger.error(f"Task {task_id} failed")
            return jsonify({"status": "failed", "task_id": task_id}), 500
        else:
            return jsonify({"status": "completed", "result": response}), 200
    except TimeoutError:
        logger.info(f"Timeout while checking task {task_id}")
        return jsonify({"status": "pending", "task_id": task_id}), 202


@app.route("/hi", methods=["GET"])
def say_hello():
    logger.info("Received request to /hi endpoint")
    return jsonify({"message": "Hello World"}), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
