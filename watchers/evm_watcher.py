import asyncio
from config import Network
from typing import List
from web3 import Web3

class EVMWatcher:
    network_id: str
    rpc_url: str
    portal_address: str
    tasks: list

    def __init__(self, network: Network):
      self.network_id = network.id
      self.rpc_url = network.rpc_url
      self.portal_address = network.portal_address
      self.tasks = []

    def getWeb3(self):
        return Web3(Web3.HTTPProvider(self.rpc_url))
    
    async def sentMessageWatcher(self):
        web3 = self.getWeb3()
        while True:
            await asyncio.sleep(5)

    async def receivedMessageWatcher(self):
        web3 = self.getWeb3()
        while True:
            await asyncio.sleep(5)

    def start(self, loop):
      print(f"Starting {self.network_id} watcher...")

      self.tasks.append(
          loop.create_task(self.sentMessageWatcher())
      )
      self.tasks.append(
          loop.create_task(self.receivedMessageWatcher())
      )

    def stop(self):
        print(f"Stopping {self.network_id} watcher...")

        for task in self.tasks:
            task.cancel()

    async def await_stopped(self):
        await asyncio.gather(*self.tasks)

        self.tasks = []
