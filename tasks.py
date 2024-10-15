import os

from celery import Celery
from dotenv import load_dotenv

# 加载 .env 文件中的环境变量
load_dotenv()

# 从环境变量获取 Redis 密码
REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD")

# 使用Redis作为消息代理和结果后端，并添加密码
app = Celery(
    "tasks",
    broker=f"redis://:{REDIS_PASSWORD}@localhost:6379/0",
    backend=f"redis://:{REDIS_PASSWORD}@localhost:6379/0",
)


@app.task
def process_task(data):
    # 这里是任务处理逻辑
    # 在实际应用中,这个任务会被本地机器执行
    result = f"Processed: {data}"
    return result


@app.task
def get_task_result(task_id):
    # 获取任务结果
    result = app.AsyncResult(task_id)
    if result.ready():
        return result.result
    else:
        return "Task is still processing..."
