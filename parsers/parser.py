from db import Message
from dataclasses import dataclass

@dataclass
class Token:
    symbol: str
    address: str
    asset_id: str

    @staticmethod
    def from_dict(data: dict) -> 'Token':
        return Token(
            symbol=data['symbol'],
            address=data['address'],
            asset_id=data.get('asset_id', '')
        )


class Parser:
    def process_message(self, db, config, message: Message) -> Message:
        pass
