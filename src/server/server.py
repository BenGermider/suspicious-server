import threading
from datetime import datetime, timedelta

import uvicorn
import ssl
import asyncio
import logging
import sys
import time
from typing import Dict, Tuple

from fastapi import FastAPI

from src.rabbitmq.rabbitmqinterface import RabbitMQInterface
from src.utils import send_log, send_command


logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

HOST = "0.0.0.0"
PORT = 8000


class Server:
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.connections: Dict[str, asyncio.StreamWriter] = {}
        self.heartbeats_timestamps: Dict[str, datetime] = {}
        self.rmq_interface = RabbitMQInterface()
        self.context = self._ssl_context()

    @staticmethod
    def _ssl_context() -> ssl.SSLContext:
        context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        context.load_cert_chain(certfile="server.crt", keyfile="server.key")
        return context

    async def _send_heartbeats(self):
        while True:
            time.sleep(10)
            for writer in self.connections.values():
                writer.write(b"heartbeat")

    async def _evict_dead(self) -> None:
        now = datetime.now()
        timeout = timedelta(seconds=30)
        for addr, last_alive in self.heartbeats_timestamps.items():
            if now - last_alive > timeout:
                conn = self.connections[addr]
                conn.close()
                del self.connections[addr]
                del self.heartbeats_timestamps[addr]


    async def _update_health(self, address) -> None:
        self.heartbeats_timestamps[address] = datetime.now()


    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        addr = str(writer.get_extra_info('peername'))  # get ip
        self.connections[addr] = writer
        send_log(f"Received connection from client {addr}", self, "debug")

        try:
            while True:
                data = await reader.read(1024)
                if not data:
                    break
                message = data.decode()
                send_log(f"Received from {addr}: {message}", self, "debug")

                if message == "alive":
                    await self._update_health(addr)

                if "command" in message:
                    send_command(command, self)


        except Exception as e:
            send_log(f"Error with client {addr}: {e}", self, "debug")


    async def start_server(self):
        await self.rmq_interface.connect()
        server = await asyncio.start_server(
            self.handle_client, self.host, self.port, ssl=self.context
        )
        send_log(f"Server listening on {self.host}:{self.port}", self, "debug")
        threading.Thread(target=self._evict_dead).start()
        async with server:
            await server.serve_forever()

    def send_command_client(self, command: str):
        cmd, addr, rest = self._digest_command(command)
        if addr not in self.connections:
            send_command(f"Received bad command: {command}, client does not exist.")
            return
        writer = self.connections[addr]
        writer.write(" ".join([cmd, rest]).encode())
        asyncio.create_task(writer.drain())

    def get_clients(self):
        return list(self.connections.keys())

    @staticmethod
    def _digest_command(command) -> Tuple[str, str, str]:
        try:
            parts = command.split(" ")
            cmd = parts[0]
            addr = parts[1]
            rest = " ".join(parts[2:])
            return cmd, addr, rest
        except Exception:
            send_command(f"Received bad command: {command}, invalid format.")


server = Server(HOST, PORT)
app = FastAPI()

@app.post("/command", status_code=200)
def command(cmd: str):
    logger.debug(f"Received: {cmd}")
    if cmd == "display clients":
        return {"result": server.get_clients()}
    return {"result": server.send_command_client(cmd)}



async def main():
    tcp_task = asyncio.create_task(server.start_server())
    api_config = uvicorn.Config(app, host="0.0.0.0", port=8000, log_level="debug")
    api_server = uvicorn.Server(api_config)
    api_task = asyncio.create_task(api_server.serve())
    await asyncio.gather(tcp_task, api_task)

if __name__ == "__main__":
    asyncio.run(main())
