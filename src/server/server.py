import socket
import logging
import sys
import ssl
import time
import threading
from fastapi import FastAPI

app = FastAPI()

# Configure basic logging
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
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.connections = {}
        self.running = False
        self.context = None

    def start(self):
        self.running = True
        threading.Thread(target=self._run, daemon=True).start()

    def _ssl_context(self):
        self.context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        self.context.load_cert_chain(certfile="server.crt", keyfile="server.key")


    def _run(self):
        s = socket.socket()
        s.bind((self.host, self.port))
        s.listen()
        logger.info(f"Server listening on {self.host}:{self.port}")
        while self.running:
            with self.context.wrap_socket(s, server_side=True) as secure_socket:
                conn, addr = secure_socket.accept()
                self.connections[addr] = conn
                logger.info(f"Received connection from client {addr}")


    def get_clients(self):
        return self.connections

    def kill_client(self, addr, retries=3):
        if retries <= 0:
            return

        if addr in self.connections:

            try:
                self.connections[addr].close()
                del self.connections[addr]
                logger.debug(f"Successfully deleted {addr}")

            except Exception as e:
                logger.debug(f"Failed to disconnect {addr} for {e}, retrying {retries - 1} times")
                self.kill_client(addr, retries - 1)


server = Server(HOST, PORT)
server.start()


def digest_commands(cmd: str):
    if cmd == "display clients":
        return server.get_clients()
    if cmd.startswith("echo"):
        return cmd[5:]
    return "Unknown command"


@app.post("/command", status_code=200)
def command(cmd: str):
    logger.debug(f"Received: {cmd}")
    result = digest_commands(cmd)
    return {"result": result}