from parsers.erc20bridge import ERC20BridgeParser
from parsers.catbridge import CATBridgeParser
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

    def parse_message(db, message):
        for parser_config in config.parsers:
            if parser_config.type == 'erc20bridge':
                message = ERC20BridgeParser.process_message(db, parser_config, message)
            elif parser_config.type == 'catbridge':
                message = CATBridgeParser.process_message(db, parser_config, message)
            else:
                logging.error(f"Unknown parser type: {parser_config.type}")
                return message

        return message

    watchers = []

    for network in config.networks:
        if network.type == NetworkType.CHIA:
            watchers.append(ChiaWatcher(network, parse_message_func=parse_message))
        elif network.type == NetworkType.EVM:
            watchers.append(EVMWatcher(network, parse_message_func=parse_message))

    loop = asyncio.get_event_loop()
    
    try:
      for watcher in watchers:
          watcher.start(loop)

      loop.run_forever()
    except KeyboardInterrupt:
      logging.info("[root] - Stopping watchers...")

      loop.run_until_complete(close_watchers(watchers))
    finally:
      loop.close()
      logging.info("[root] - Done.")

if __name__ == "__main__":
    watch()
