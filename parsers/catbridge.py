from db import Message, split_message_contents, increment_key_value
from chia.util.bech32m import encode_puzzle_hash
from parsers.parser import Parser, Token
from dataclasses import dataclass
from typing import List
import json
import web3

@dataclass
class CATBridgeConfig:
    type: str
    chains: List[str]
    tokens: List[Token]

    @staticmethod
    def from_dict(data: dict) -> 'CATBridgeConfig':
        return CATBridgeConfig(
            type='catbridge',
            chains=data['chains'],
            tokens=[Token.from_dict(token) for token in data['tokens']]
        )
     

class CATBridgeParser(Parser):
  @staticmethod
  def process_message(db, config: CATBridgeConfig, message: Message) -> Message:
        if message.source_chain.decode() not in config.chains or message.destination_chain.decode() not in config.chains:
            return message
        
        evm_chain = config.chains[0] if config.chains[1] == "xch" else config.chains[1]
        from_evm = message.source_chain.decode() == evm_chain

        contract_address = message.source.hex() if from_evm else message.destination.hex()
        token = None
        for tk in config.tokens:
            if tk.contract.lower().replace('0x', '') == contract_address:
                token = tk
                break
            
        if token is None:
            return message
        
        contents = split_message_contents(message.contents)
        receiver = contents[0].hex()
        amount = int(contents[1].hex(), 16)

        if from_evm:
            receiver = encode_puzzle_hash(bytes.fromhex(receiver), 'xch')
        else:
            receiver = web3.Web3.to_checksum_address('0x' + receiver[-40:])

        increment_key_value(db, f"{token.symbol}_total_volume", amount)
        increment_key_value(db, f"{token.symbol}_locked", -amount if from_evm else amount)
        message.parsed = json.dumps({
            'type': 'cat_bridge',
            'contract': token.contract,
            'asset_id': token.asset_id,
            'token_symbol': token.symbol,
            'amount_mojo': amount,
            'receiver': receiver
        })
        return message
