import json
from typing import Any

import elasticsearch
from elasticsearch import Elasticsearch, AsyncElasticsearch
import os

class ESInterface:

    def __init__(self):
        self.es = AsyncElasticsearch(
            f"http://{os.getenv('ES_HOST', 'localhost')}:{os.getenv('ES_PORT', 9200)}",
        )

        if not self.es.indices.exists(index="commands"):
            self.es.indices.create(index="commands")

        if not self.es.indices.exists(index="logs"):
            self.es.indices.create(index="logs")

    async def save_in_es(self, ch, method, properties, body):
        try:
            data = json.loads(body)

            if "logs" in data:
                await self._save_log(data)
            if "command" in data:
                await self._save_command(body)
            ch.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as e:
            ch.basic_nack(delivery_tag=method.delivery_tag)


    async def _save_log(self, log: Any) -> None:
        await self.es.index(
            index="logs",
            document=log
        )

    async def _save_command(self, command: Any) -> None:
        await self.es.index(
            index="logs",
            document=command
        )
