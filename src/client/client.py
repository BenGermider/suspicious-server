import socket
import logging
import os
import ssl
import sys
import threading
import time


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

    def start(self):
        self._connect()
        threading.Thread(target=self.read_commands).start()

    def _connect(self):
        self.socket.connect((self.host, self.port))

    def read_commands(self):
        while True:
            with self.context.wrap_socket(self.socket, server_hostname=self.host) as secure_socket:
                command = secure_socket.recv(1024)
                logger.debug(f"Received {command}")
                self._obey(command)

    def _obey(self, command):
        succeeded = 1
        try:
            if command.lower().startswith("echo"):
                command = command.replace("echo", "", 1)
                print(command)
        finally:
            self.socket.send(f"Report {succeeded}".encode())

def main():

    s = socket.socket()
    s.connect((HOST, PORT))
    time.sleep(5)
    s.close()


if __name__ == "__main__":
    main()
