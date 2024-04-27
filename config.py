import json
from dataclasses import dataclass, field
from typing import List, Optional
import enum
import sys
import os

class NetworkType(enum.Enum):
    COINSET = "coinset"
    EVM = "evm"


@dataclass
class Network:
    id: str
    type: NetworkType
    rpc_url: str
    portal_launcher_id: Optional[str] = None
    bridging_puzzle_hash: Optional[str] = None
    portal_address: Optional[str] = None


@dataclass
class Config:
    networks: List[Network] = field(default_factory=list)

    @staticmethod
    def load() -> 'Config':
        try:
          json_data = json.loads(
              open("config.json", "r").read()
          )
        except:
          print("Could not load config from config.json")
          sys.exit(1)
        
        config = Config()
        for net in json_data.get('networks', []):
            network_id: str = net['id']

            rpc_url_env_variable = f"{network_id.upper()}_RPC_URL"
            rpc_url = os.environ.get(rpc_url_env_variable)
            if rpc_url is None:
               print(f"Could not load RPC URL for '{network_id}' from environment variable {rpc_url_env_variable}")
               sys.exit(1)

            network = Network(
                id=net['id'],
                type=NetworkType(net['type']),
                rpc_url=rpc_url,
                portal_launcher_id=net.get('portal_launcher_id'),
                bridging_puzzle_hash=net.get('bridging_puzzle_hash'),
                portal_address=net.get('portal_address')
            )
            config.networks.append(network)
        return config
