import datetime
import json
import os
from time import perf_counter

from celery import Celery
from celery.exceptions import SoftTimeLimitExceeded
from celery.signals import (
    task_postrun,
    task_prerun,
    worker_process_init,
    worker_shutdown,
    worker_shutting_down,
)
import hashlib
import redis
from dotenv import load_dotenv
from loguru import logger
from utils import get_openai_prompt, grpc_request, upload_image_to_oss

logger.remove()

# 日志配置（延迟到 worker 进程初始化时配置，避免 fork 问题）
LOG_CONFIG = {
    "sink": "./celery_logs/out_1.log",
    "rotation": "03:00",
    "retention": "10 days",
    "backtrace": True,
    "diagnose": True,
    "level": "DEBUG",
    "encoding": "utf-8",
    "enqueue": True,  # 关键：使用队列确保多进程安全
}

# 加载 .env 文件中的环境变量
load_dotenv()

# 从环境变量获取 Redis 密码
REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD")
REDIS_HOST = os.environ.get("REDIS_HOST")

# Redis 缓存客户端（用于结果缓存）
redis_client = redis.Redis(
    host=REDIS_HOST,
    port=6379,
    password=REDIS_PASSWORD,
    db=1,  # 使用不同的 db 避免和 Celery broker 冲突
    decode_responses=True,
)

# 缓存过期时间（秒）
CACHE_EXPIRE = 3600 * 24  # 24 小时


def get_cache_key(data: dict) -> str:
    """根据关键参数生成缓存 key"""
    # 只用影响生成结果的参数来计算 hash
    key_data = {
        "gpt_prompt": data.get("gpt", {}).get("gpt_prompt", ""),
        "user_prompt": data.get("user_prompts", [{}])[0].get("prompt", ""),
        "template_prompt": data.get("template", {}).get("prompt", ""),
        "gender": data.get("user", {}).get("gender", "男"),
        "ages": data.get("user", {}).get("ages", "18-30"),
    }
    key_str = json.dumps(key_data, sort_keys=True, ensure_ascii=False)
    return f"kolors:cache:{hashlib.md5(key_str.encode()).hexdigest()}"


app = Celery(
    "tasks",
    broker=f"redis://:{REDIS_PASSWORD}@{REDIS_HOST}:6379/0",
    backend=f"redis://:{REDIS_PASSWORD}@{REDIS_HOST}:6379/0",
)

app.conf.update(
    broker_connection_retry_on_startup=True,
    broker_connection_max_retries=None,
    task_track_started=True,
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    # 设置任务软时间限制
    task_soft_time_limit=3600,  # 1小时
    # 设置任务硬时间限制
    task_time_limit=3600 + 300,  # 1小时5分钟
    # 设置优雅关闭超时
    worker_shutdown_timeout=300,  # 5分钟
    # === 单 GPU 优化配置 ===
    worker_concurrency=1,         # 只允许同时执行 1 个任务
    worker_prefetch_multiplier=1,  # 每次只预取 1 个任务，避免任务堆积在 worker 内存
)

# 启动 Banner
BANNER = """
\033[38;5;213m
  ██╗  ██╗ ██████╗ ██╗      ██████╗ ██████╗ ███████╗
  ██║ ██╔╝██╔═══██╗██║     ██╔═══██╗██╔══██╗██╔════╝
  █████╔╝ ██║   ██║██║     ██║   ██║██████╔╝███████╗
  ██╔═██╗ ██║   ██║██║     ██║   ██║██╔══██╗╚════██║
  ██║  ██╗╚██████╔╝███████╗╚██████╔╝██║  ██║███████║
  ╚═╝  ╚═╝ ╚═════╝ ╚══════╝ ╚═════╝ ╚═╝  ╚═╝╚══════╝
\033[0m
\033[38;5;51m  ╔══════════════════════════════════════════════════╗
  ║\033[0m  \033[38;5;226m⚡ GPU Image Generation Worker\033[0m                   \033[38;5;51m║
  ║\033[0m  \033[38;5;46m🚀 Powered by Celery + Redis\033[0m                     \033[38;5;51m║
  ║\033[0m  \033[38;5;208m🎨 Ready to create amazing images!\033[0m               \033[38;5;51m║
  ╚══════════════════════════════════════════════════╝\033[0m
"""

print(BANNER)

# 主进程也配置日志
logger.add(**LOG_CONFIG)
logger.info("Celery worker starting...")


@worker_process_init.connect
def init_worker(**kwargs):
    """Worker 子进程初始化时配置日志"""
    logger.remove()  # 移除之前的 handler
    logger.add(**LOG_CONFIG)
    logger.info("Worker process initialized")


@worker_shutting_down.connect
def shutdown_worker(**kwargs):
    logger.info("Worker shutting down...")


@worker_shutdown.connect
def shutdown_complete(**kwargs):
    logger.info("Worker shutdown complete")


@task_prerun.connect
def task_prerun_handler(task_id, task, *args, **kwargs):
    logger.info(f"Starting task {task.name}[{task_id}]")


@task_postrun.connect
def task_postrun_handler(task_id, task, *args, **kwargs):
    logger.info(f"Completed task {task.name}[{task_id}]")


@app.task(
    name="tasks.process_task",
    bind=True,
    soft_time_limit=300,       # 软限制 5 分钟，触发 SoftTimeLimitExceeded 异常
    time_limit=330,            # 硬限制 5.5 分钟，强制终止任务
    acks_late=True,            # 任务完成后才确认，worker 崩溃时任务会重新分配
    reject_on_worker_lost=True,  # worker 丢失时拒绝任务，配合 acks_late 使用
    max_retries=3,             # 最大重试次数
    default_retry_delay=10,    # 重试间隔（秒）
)
def process_task(self, data):
    """耗时任务处理逻辑"""
    task_id = data.get("task_id", self.request.id)
    start_time = perf_counter()

    try:
        # 检查缓存
        cache_key = get_cache_key(data)
        cached_result = redis_client.get(cache_key)
        if cached_result:
            logger.info(f"[{task_id}] 命中缓存，直接返回")
            return json.loads(cached_result)

        logger.info(f"[{task_id}] 成功接受任务")
        logger.info(json.dumps(data, ensure_ascii=False, indent=2))

        prompt_gpt = data["gpt"]["gpt_prompt"]
        prompt_user = data["user_prompts"][0]["prompt"]
        positive_prompt = data["template"]["prompt"]
        gender = data["user"].get("gender", "男")
        ages = data["user"].get("ages", "18-30")

        last_prompt1 = ""
        if len(data["user_prompts"]) > 1:
            last_prompt1 = data["user_prompts"][1].get("generate_prompt") or ""
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

        # 缓存成功的结果
        if result_data.get("status") == 200 and result_data.get("image_url"):
            try:
                redis_client.setex(cache_key, CACHE_EXPIRE, json.dumps(result_data))
                logger.info(f"[{task_id}] 结果已缓存")
            except Exception as e:
                logger.warning(f"[{task_id}] 缓存写入失败: {e}")

        return result_data

    except SoftTimeLimitExceeded:
        logger.warning(f"[{task_id}] 任务超时 (soft limit)，耗时: {perf_counter() - start_time:.2f}s")
        return {
            "chat_result": None,
            "image_url": None,
            "status": "timeout",
            "error": "任务执行超时",
        }

    except Exception as exc:
        logger.error(f"[{task_id}] 任务执行失败: {exc}")
        # 可选：自动重试
        # raise self.retry(exc=exc, countdown=10)
        return {
            "chat_result": None,
            "image_url": None,
            "status": "error",
            "error": str(exc),
        }


