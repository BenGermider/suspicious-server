import subprocess
from src.cli.cli import main as cli_main


def main():
    client_server_commands = ["docker-compose", "up"]
    subprocess.run(
        client_server_commands
    )
    cli_main()


if __name__ == "__main__":
    main()
