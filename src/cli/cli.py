import requests
import os

def main():
    while True:
        task = input("> ")
        host = os.getenv("HOST", "localhost")
        port = os.getenv("PORT", "8000")
        url = f"http://{host}:{port}/command"
        r = requests.Request(
            url, task
        )
        print(r.json())

if __name__ == "__main__":
    main()
