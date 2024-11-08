import os
import datetime
import json
from time import perf_counter
import time

from celery import Celery
from dotenv import load_dotenv
from loguru import logger

from utils import get_openai_prompt, upload_image_to_oss, grpc_request

logger.remove()
logger.add(
    "./celery_logs/out_1.log",
    rotation="03:00",
    retention="10 days",
    backtrace=True,
    diagnose=True,
    level="DEBUG",
    encoding="utf-8",
)

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

app.conf.update(
    broker_connection_retry_on_startup=True,
    broker_connection_max_retries=None,
    task_track_started=True,
    task_serializer='json',
    result_serializer='json',
    accept_content=['json']
)

# 添加这些日志来检查连接信息
logger.info("Celery worker starting...")

@app.task(name="tasks.process_task")
def process_task(data):
    logger.info("Received process_task request")
    result = long_running_task.apply_async(args=[data])
    return result.id


@app.task
def long_running_task(data):
    # 这里是实际的耗时任务处理逻辑

    task_id = data["task_id"]
    start_time = perf_counter()
    logger.info(f"[{task_id}] 成功接受任务")
    logger.info(json.dumps(data, ensure_ascii=False, indent=2))

    prompt_gpt = data["gpt"]["gpt_prompt"]
    prompt_user = data["user_prompts"][0]["prompt"]
    positive_prompt = data["template"]["prompt"]
    gender = data["user"].get("gender", "男")
    ages = data["user"].get("ages", "18-30")

    last_prompt1 = ""
    if len(data["user_prompts"]) > 1:
        last_prompt1 = data["user_prompts"][1].get("generate_prompt", "")
    prompt_gpt = prompt_gpt.replace("{last1}", last_prompt1)
    prompt_gpt = prompt_gpt.replace("{my_info}", f"性别为{gender},年龄在{ages}左右.")

    prompt, status_code = get_openai_prompt(prompt_gpt + prompt_user)
    gpt_end_time = perf_counter()
    logger.info(f"[{task_id}] GPT翻译成功")

    json_data = {
        "prompt": positive_prompt + prompt,
        "num_inference_steps": 20,
        "guidance_scale": 5.0,
        "height": 896,
        "width": 896,
    }

    image_bytes = grpc_request(json_data)
    comfyui_end_time = perf_counter()
    logger.info(f"[{task_id}] ComfyUI生成成功")
    if image_bytes:
        oss_image_url = upload_image_to_oss(
            image_bytes, datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S-%f")
        )
    else:
        oss_image_url = None

    result_data = {
        "chat_result": prompt,
        "image_url": oss_image_url,
        "status": status_code,
    }
    logger.info(
        f"""
[{task_id}]: {perf_counter() - start_time:.2f} | GPT: {gpt_end_time - start_time:.2f} | ComfyUI: {comfyui_end_time - start_time:.2f}
结果: {json.dumps(result_data, ensure_ascii=False, indent=4)}"""
    )

    return result_data


@app.task(name="tasks.get_task_result")
def get_task_result(task_id):
    # 获取任务结果
    result = app.AsyncResult(task_id)
    if result.ready():
        return result.result
    else:
        return "Task is still processing..."
