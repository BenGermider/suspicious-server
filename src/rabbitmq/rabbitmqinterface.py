import asyncio
import json
import logging
import os
import sys
from typing import Any, Awaitable, Callable, Union

import aio_pika

# Configure basic logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[logging.StreamHandler(sys.stdout)]
)

logger = logging.getLogger(__name__)


class RabbitMQInterface:

    def __init__(self):
        self.conn = None
        self.channel = None
        self.queue = None
        self.exchange = None

    async def connect(
            self,
            retries=5,
            url=None
    ) -> None:

        """
        Create a robust connection to the rabbitmq broker, declare a channel and it's settings.
        :return:
        """

        if url is None:
            rabbitmq_host = os.getenv("RABBITMQ_HOST", "localhost")
            url = f"amqp://{{user}}:{{password}}@{rabbitmq_host}:{{port}}/"

        if retries <= 0:
            await self.disconnect()
            raise RuntimeError(f"Failed to connect to rabbitmq")

        url = url.format(
            user=os.getenv("RABBITMQ_USER", "localhost"),
            password=os.getenv("RABBITMQ_PASS", "12345"),
            port=os.getenv("RABBITMQ_PORT", 5672)
        )
        try:

            logger.debug(f"Trying to connect to {url}")
            self.conn = await aio_pika.connect_robust(url, timeout=5)  # WHILE DEBUGGING AND RUNNING LOCALLY, CHANGE rabbitmq TO localhost
            self.channel = await self.conn.channel()
            await self.channel.set_qos(prefetch_count=1)
            logging.debug(f"Successfully connected to rabbitmq")

        except Exception as e:

            logging.exception(f"Encountered exception while connecting to rabbitmq, retrying...")
            await asyncio.sleep(3)
            await self.connect(retries=retries - 1, url=url)

    async def disconnect(self) -> None:
        """
        Disconnect from the broker
        :return:
        """
        if self.channel and not self.channel.is_closed():
            await self.channel.close()
        if self.conn and not self.conn.is_closed():
            await self.conn.close()

    async def _setup(
            self,
            queue_name: str,
            exchange_name: str = "default",
            exchange_type: aio_pika.ExchangeType = aio_pika.ExchangeType.DIRECT
    ) -> None:
        """
        In case of any change of the queue/exchange used, apply the changes
        :param queue_name:
        :param exchange_name:
        :param exchange_type:
        :return:
        """
        if not self.queue or self.queue.name != queue_name:
            self.queue = await self.channel.declare_queue(
                name=queue_name,
                durable=True
            )

        if not self.exchange or self.exchange.name != exchange_name:
            self.exchange = await self.channel.declare_exchange(
                name=exchange_name,
                type=exchange_type
            )

        await self.queue.bind(self.exchange, routing_key=queue_name)

    async def publish(
            self,
            queue: str,
            data: Any,
            exchange_type=aio_pika.ExchangeType.DIRECT,
            exchange_name="default"
    ) -> None:
        """
        Publish a message to the desired queue with a requested exchange.
        :param queue:
        :param data:
        :param exchange_type:
        :param exchange_name:
        :return:
        """
        await self._setup(queue, exchange_name, exchange_type)

        try:
            msg = aio_pika.Message(body=json.dumps(data).encode(), delivery_mode=aio_pika.DeliveryMode.PERSISTENT)
            await self.exchange.publish(msg, routing_key=queue)

        except TypeError:
            logger.error(f"Data {data} not serializable")

        except aio_pika.AMQPException as e:
            logger.error(f"Failed to send message to {queue}, reason: {e}")

        except Exception as e:
            logger.error(f"Unexpected error: {e}")

    async def consume(self,
                      queue: str,
                      callback: Union[Callable, Awaitable]
                      ) -> None:
        """
        Consumes a message from queue and runs callback func on it.
        :param queue:
        :param callback:
        :return:
        """
        await self._setup(queue)

        try:
            async with self.queue.iterator() as queue_iter:
                async for message in queue_iter:
                    async with message.process():
                        data = message.body.decode()
                        await callback(json.loads(data))

        except Exception as e:
            logger.error(f"Failed to consume a message from {queue}\nreason:{e}\ndata:{data}")
            return