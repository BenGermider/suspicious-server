import json
import socket
import logging
import os
import ssl
import sys
import threading
import time

import asyncio

from src.rabbitmq.rabbitmqinterface import RabbitMQInterface
from src.utils import send_log

# Configure basic logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[logging.StreamHandler(sys.stdout)]
)

logger = logging.getLogger(__name__)

HOST = os.getenv("HOST", "server")
PORT = os.getenv("PORT", 8000)


class Client:

    def __init__(self):
        self.host = HOST
        self.port = PORT
        self.socket = socket.socket()
        self.context = ssl.create_default_context()
        self.rmq_interface = RabbitMQInterface()

    def start(self):
        self._connect()
        threading.Thread(target=self.read_commands).start()

    def _connect(self):
        self.socket.connect((self.host, self.port))
        asyncio.run(self.rmq_interface.connect())


    def read_commands(self):
        while True:
            with self.context.wrap_socket(self.socket, server_hostname=self.host) as secure_socket:
                command = secure_socket.recv(1024)
                send_log(f"Client received {command}", self, "debug")

                command = command.decode()
                if command == "heartbeat":
                    self.socket.send(b"alive")
                else:
                    self._obey(command)

    def _obey(self, command):
        succeeded = 1  # Basic output
        try:
            if command.lower().startswith("echo"):
                command = command.replace("echo", "", 1)
                print(command)
        finally:
            self.socket.send(json.dumps({"command": command, "output": succeeded}).encode())

def main():

    s = socket.socket()
    s.connect((HOST, PORT))
    time.sleep(5)
    s.close()


if __name__ == "__main__":
    main()
