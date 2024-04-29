from db import Message
from dataclasses import dataclass

@dataclass
class Token:
    symbol: str
    contract: str
    asset_id: str

    @staticmethod
    def from_dict(data: dict) -> 'Token':
        return Token(
            symbol=data['symbol'],
            contract=data['contract'],
            asset_id=data.get('asset_id', '')
        )


class Parser:
    @staticmethod
    def process_message(db, config, message: Message) -> Message:
        pass
