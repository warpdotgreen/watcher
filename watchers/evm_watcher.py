import asyncio
from config import Network
from typing import List
from web3 import Web3
from db import setup_database

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
    
    def getDb(self):
        return setup_database()
    
    def log(self, message):
        print(f"[{self.network_id} watcher] {message}")
    
    async def sentMessageWatcher(self):
        web3 = self.getWeb3()
        db = self.getDb()

        while True:
            await asyncio.sleep(5)

    async def receivedMessageWatcher(self):
        web3 = self.getWeb3()
        db = self.getDb()
        
        while True:
            await asyncio.sleep(5)

    def start(self, loop):
      self.log(f"Starting...")

      self.tasks.append(
          loop.create_task(self.sentMessageWatcher())
      )
      self.tasks.append(
          loop.create_task(self.receivedMessageWatcher())
      )

    def stop(self):
        self.log(f"Stopping...")

        for task in self.tasks:
            task.cancel()

    async def await_stopped(self):
        await asyncio.gather(*self.tasks)

        self.tasks = []
