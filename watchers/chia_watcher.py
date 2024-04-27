import asyncio
import aiohttp
from config import Network
from chia.rpc.full_node_rpc_client import FullNodeRpcClient
from chia.types.blockchain_format.sized_bytes import bytes32
from typing import List

class HTTPFullNodeRpcClient(FullNodeRpcClient):
    def __init__(self, base_url: str):
        super().__init__(None, None, None, None, None, None)
        self.session = aiohttp.ClientSession()
        self.closing_task = None
        self.base_url = base_url

    async def fetch(self, path, request_json):
        async with self.session.post(f"{self.base_url}/{path}", json=request_json) as response:
            response.raise_for_status()

            res_json = await response.json()
            if not res_json["success"]:
                raise ValueError(res_json)
            return res_json

    def close(self):
        self.closing_task = self.session.close()

    async def await_closed(self):
        await self.closing_task
        

class ChiaWatcher:
    network_id: str
    rpc_url: str
    portal_launcher_id: bytes32
    tasks: list
    nodes: List[HTTPFullNodeRpcClient]

    def __init__(self, network: Network):
      self.network_id = network.id
      self.rpc_url = network.rpc_url
      self.portal_launcher_id = bytes.fromhex(network.portal_launcher_id)
      self.tasks = []
      self.nodes = []

    def getNode(self):
        node = HTTPFullNodeRpcClient(self.rpc_url)
        self.nodes.append(node)
        return node
    
    async def sentMessageWatcher(self):
        while True:
            await asyncio.sleep(5)

    async def receivedMessageWatcher(self):
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

        for node in self.nodes:
            node.close()

        for node in self.nodes:
            await node.await_closed()

        self.tasks = []
        self.nodes = []
