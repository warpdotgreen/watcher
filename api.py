from db import Message, setup_database, split_message_contents, KeyValueEntry
from hypercorn.config import Config as HyperConfig
from fastapi import FastAPI, HTTPException, Query
from starlette.responses import JSONResponse
from hypercorn.asyncio import serve
from sqlalchemy.orm import Session
from sqlalchemy import desc
import asyncio
import json

app = FastAPI()

@app.get("/stats")
async def read_stats():
    db: Session = setup_database()
    total_messages = db.query(Message).count()
    messages_to_chia = db.query(Message).filter(Message.destination_chain == b'xch').count()
    messages_from_chia = db.query(Message).filter(Message.source_chain == b'xch').count()
    resp = {
        "total_messages": total_messages,
        "messages_to_chia": messages_to_chia,
        "messages_from_chia": messages_from_chia,
    }

    kvs = db.query(KeyValueEntry).all()
    for kv in kvs:
        resp[kv.key] = kv.value_int

    return resp


@app.get("/messages")
async def read_messages(
    id: int = None,
    nonce: str = None,
    from_id: int = None,
    to_id: int = None,
    limit: int = Query(10, le=100),
    offset: int = 0,
    from_source_block_number: int = None,
    to_source_block_number: int = None,
    from_destination_block_number: int = None,
    to_destination_block_number: int = None,
    order_by: str = Query("id", pattern="^(id|source_block_number|destination_block_number)$"),
    sort: str = Query("desc", pattern="^(asc|desc)$"),
    source_chain: str = None,
    destination_chain: str = None,
    status: str = None,
    source: str = None,
    destination: str = None
):
    db: Session = setup_database()
    query = db.query(Message)
    
    if id:
        query = query.filter(Message.id == id)
    if nonce:
        query = query.filter(Message.nonce == bytes.fromhex(nonce))
    if from_id and to_id:
        query = query.filter(Message.id >= from_id, Message.id <= to_id)
    if from_source_block_number and to_source_block_number:
        query = query.filter(Message.source_block_number >= from_source_block_number, Message.source_block_number <= to_source_block_number)
    if from_destination_block_number and to_destination_block_number:
        query = query.filter(Message.destination_block_number >= from_destination_block_number, Message.destination_block_number <= to_destination_block_number)
    if source_chain:
        query = query.filter(Message.source_chain == source_chain.encode())
    if destination_chain:
        query = query.filter(Message.destination_chain == destination_chain.encode())
    if status:
        query = query.filter(Message.status == status)
    if source:
        query = query.filter(Message.source == bytes.fromhex(source))
    if destination:
        query = query.filter(Message.destination == bytes.fromhex(destination))
    
    if order_by:
        order_column = getattr(Message, order_by)
        query = query.order_by(order_column.desc() if sort == "desc" else order_column.asc())
    
    messages = query.offset(offset).limit(limit).all()
    return [process_message(msg) for msg in messages]


def process_message(message: Message):
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
        "status": message.status.value,
        "parsed": json.loads(message.parsed) if message.parsed is not None else {}
    }


async def start_api():
    hyper_config = HyperConfig()
    hyper_config.bind = ["0.0.0.0:8000"]
    await serve(app, hyper_config)


if __name__ == "__main__":
    asyncio.run(start_api())
