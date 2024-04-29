from db import Message, split_message_contents, increment_key_value
from chia.util.bech32m import encode_puzzle_hash
from parsers.parser import Parser, Token
from dataclasses import dataclass
from typing import List
import json
import web3

@dataclass
class ERC20BridgeConfig:
    type: str
    chains: List[str]
    contract: str
    tokens: List[Token]

    @staticmethod
    def from_dict(data: dict) -> 'ERC20BridgeConfig':
        return ERC20BridgeConfig(
            type='erc20bridge',
            chains=data['chains'],
            contract=data['contract'],
            tokens=[Token.from_dict(token) for token in data['tokens']]
        )
     

class ERC20BridgeParser(Parser):
  @staticmethod
  def process_message(db, config: ERC20BridgeConfig, message: Message) -> Message:
        if message.source_chain.decode() not in config.chains or message.destination_chain.decode() not in config.chains:
            return message
        
        evm_chain = config.chains[0] if config.chains[1] == "xch" else config.chains[1]
        from_evm = message.source_chain.decode() == evm_chain

        if from_evm and '0x' + message.source.hex() != config.contract.lower():
            return message
        
        if not from_evm and '0x' + message.destination.hex() != config.contract.lower():
            return message
        
        contents = split_message_contents(message.contents)
        asset_contract_address = contents[0].hex()
        receiver = contents[1].hex()
        amount = int(contents[2].hex(), 16)

        while asset_contract_address.startswith('00'):
            asset_contract_address = asset_contract_address[2:]

        if from_evm:
            receiver = encode_puzzle_hash(bytes.fromhex(receiver), 'xch')
        else:
            receiver = web3.Web3.to_checksum_address('0x' + receiver[-40:])

        token = None
        for t in config.tokens:
            if t.contract.lower() == '0x' + asset_contract_address:
                token = t
                break
            
        if token is None:
            return message

        increment_key_value(db, f"{token.symbol}_total_volume", amount)
        increment_key_value(db, f"{token.symbol}_locked", amount if from_evm else -amount)
        message.parsed = json.dumps({
            'type': 'erc20_bridge',
            'contract': token.contract,
            'asset_id': token.asset_id,
            'token_symbol': token.symbol,
            'amount_mojo': amount,
            'receiver': receiver
        })
        return message
