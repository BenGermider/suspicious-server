import logging
import sys
from typing import Any, Optional
import asyncio
from src.consts import es_queue

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[logging.StreamHandler(sys.stdout)]
)

logger = logging.getLogger(__name__)


def send_log(log: str, obj: Any, level: str):
    output = {"log": log}
    if level == "debug":
        logging.debug(
            str
        )
        output.update({"level": "debug"})

    if level == "info":
        logging.info(
            str
        )
        output.update({"level": "debug"})

    asyncio.run(obj.rmq_interface.publish(
        es_queue, output
    ))

def send_command(command, obj: Any):
    asyncio.run(obj.rmq_interface.publish(
        es_queue, command
    ))