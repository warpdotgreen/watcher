from db import Message, setup_database, split_message_contents
from hypercorn.config import Config as HyperConfig
from fastapi import FastAPI, HTTPException, Query
from starlette.responses import JSONResponse
from hypercorn.asyncio import serve
from sqlalchemy.orm import Session
from sqlalchemy import desc
import asyncio

app = FastAPI()

@app.get("/messages")
async def read_messages(
    id: int = None,
    nonce: str = None,
    from_id: int = None,
    to_id: int = None,
    limit: int = 10
):
    db: Session = setup_database()
    query = db.query(Message)
    if id:
        message = query.filter(Message.id == id).first()
        if message:
            return process_message(message)
        raise HTTPException(status_code=404, detail="Message not found")
    if nonce:
        messages = query.filter(Message.nonce == bytes.fromhex(nonce)).all()
        return [process_message(msg) for msg in messages]
    if from_id and to_id:
        messages = query.filter(Message.id >= from_id, Message.id <= to_id).all()
        return [process_message(msg) for msg in messages]
    
    if limit > 100:
        limit = 100
    messages = query.order_by(desc(Message.id)).limit(limit).all()
    return [process_message(msg) for msg in messages]


def process_message(message):
    return {
        "id": message.id,
        "nonce": message.nonce.hex(),
        "source_chain": message.source_chain.decode(),
        "source": message.source.hex(),
        "destination_chain": message.destination_chain.decode(),
        "destination": message.destination.hex(),
        "contents": [_.hex() for _ in split_message_contents(message.contents)],
        "source_block_number": message.source_block_number,
        "source_timestamp": message.source_timestamp,
        "source_transaction_hash": message.source_transaction_hash.hex(),
        "destination_block_number": message.destination_block_number,
        "destination_timestamp": message.destination_timestamp,
        "destination_transaction_hash": message.destination_transaction_hash.hex() if message.destination_transaction_hash is not None else None,
        "status": message.status.value
    }


async def start_api():
    hyper_config = HyperConfig()
    hyper_config.bind = ["0.0.0.0:8000"]
    await serve(app, hyper_config)


if __name__ == "__main__":
    asyncio.run(start_api())
