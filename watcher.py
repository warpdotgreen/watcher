from watchers.chia_watcher import ChiaWatcher
from watchers.evm_watcher import EVMWatcher
from config import Config, NetworkType
from dotenv import load_dotenv
import asyncio
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

async def close_watchers(watchers):
    for watcher in watchers:
        watcher.stop()

    for watcher in watchers:
        try:
          await watcher.await_stopped()
        except asyncio.CancelledError:
           pass

def watch():
    load_dotenv()
    config: Config = Config.load()

    watchers = []

    for network in config.networks:
        if network.type == NetworkType.CHIA:
            watchers.append(ChiaWatcher(network))
        elif network.type == NetworkType.EVM:
            watchers.append(EVMWatcher(network))

    loop = asyncio.get_event_loop()
    for watcher in watchers:
        watcher.start(loop)
    
    try:
      loop.run_forever()
    except KeyboardInterrupt:
      logging.info("[root] - Stopping watchers...")

      loop.run_until_complete(close_watchers(watchers))
    finally:
      loop.close()
      logging.info("[root] - Done.")

if __name__ == "__main__":
    watch()
