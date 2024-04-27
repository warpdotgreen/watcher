from config import Config
from dotenv import load_dotenv
import json

def watch():
  load_dotenv()
  config: Config = Config.load()

  print(config)


if __name__ == "__main__":
  watch()
