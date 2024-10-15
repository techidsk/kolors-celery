import os
import time

from celery import Celery
from dotenv import load_dotenv

# 加载 .env 文件中的环境变量
load_dotenv()

# 从环境变量获取 Redis 密码
REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD")
REDIS_HOST = os.environ.get("REDIS_HOST")
app = Celery(
    "tasks",
    broker=f"redis://:{REDIS_PASSWORD}@{REDIS_HOST}:6379/0",
    backend=f"redis://:{REDIS_PASSWORD}@{REDIS_HOST}:6379/0",
)


@app.task
def process_task(data):
    # 创建一个新的异步任务来处理耗时操作
    result = long_running_task.apply_async(args=[data])
    return result.id


@app.task
def long_running_task(data):
    # 这里是实际的耗时任务处理逻辑
    time.sleep(10)  # 模拟耗时操作，实际中替换为您的处理逻辑
    result = f"Processed by local machine after long operation: {data}"
    return result


@app.task
def get_task_result(task_id):
    # 获取任务结果
    result = app.AsyncResult(task_id)
    if result.ready():
        return result.result
    else:
        return "Task is still processing..."
