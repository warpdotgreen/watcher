from watchers.chia_watcher import ChiaWatcher
from watchers.evm_watcher import EVMWatcher
from config import Config, NetworkType
from dotenv import load_dotenv
import asyncio

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
            # watchers.append(EVMWatcher(network))
            pass

    loop = asyncio.get_event_loop()
    for watcher in watchers:
        watcher.start(loop)
    
    try:
      loop.run_forever()
    except KeyboardInterrupt:
      print("Stopping watchers...")

      loop.run_until_complete(close_watchers(watchers))
    finally:
      loop.close()


if __name__ == "__main__":
    watch()
