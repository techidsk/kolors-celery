import time

import httpx
import schedule
from loguru import logger


def send_get_request(url):
    logger.info(f"Sending GET request to {url}")
    try:
        with httpx.Client() as client:
            response = client.get(url)
            logger.info(
                f"GET request sent to {url}. Status code: {response.status_code}"
            )
    except Exception as e:
        logger.error(f"Error sending GET request to {url}: {str(e)}")


def schedule_task():
    # 设置目标URL
    target_url = "https://www.ziyouxiezuo.com/api/jobs"

    # 设置任务每小时执行一次
    schedule.every(20).minutes.do(send_get_request, url=target_url)

    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    schedule_task()
