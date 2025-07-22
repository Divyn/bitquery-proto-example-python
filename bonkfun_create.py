# -*- coding: utf-8 -*-
import logging
import threading
import os
import base58
from confluent_kafka import Consumer
from google.protobuf.message import DecodeError
from queue import Queue, Empty

import config
from solana import token_block_message_pb2

# Constants
SPL_TOKEN_PROGRAM_ID = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
SPL_TOKEN_PROGRAM_ID_BYTES = base58.b58decode(SPL_TOKEN_PROGRAM_ID)
INITIALIZE_MINT_PREFIX = b"\x00\x00\x00\x00"
TARGET_METHODS = {"initialize", "initializePosition", "create"}

# Shared queue and state
message_queue = Queue()
deduped_keys = set()
dedup_lock = threading.Lock()

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(levelname)s] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def create_consumer() -> Consumer:
    conf = {
        'bootstrap.servers': config.KAFKA_BOOTSTRAP_SERVERS,
        'group.id': config.KAFKA_GROUP_ID,
        'session.timeout.ms': 30000,
        'security.protocol': 'SASL_PLAINTEXT',
        'ssl.endpoint.identification.algorithm': 'none',
        'sasl.mechanisms': 'SCRAM-SHA-512',
        'sasl.username': config.KAFKA_USERNAME,
        'sasl.password': config.KAFKA_PASSWORD,
        'auto.offset.reset': 'latest',
        'client.id': f"{config.KAFKA_USERNAME}-{os.getpid()}"
    }
    return Consumer(conf)

def quick_relevance_check(payload: bytes) -> bool:
    return SPL_TOKEN_PROGRAM_ID_BYTES in payload

def process_kafka_message(payload: bytes, _):
    try:
        tx_block = token_block_message_pb2.TokenBlockMessage()
        tx_block.ParseFromString(memoryview(payload))
        block_slot = tx_block.Header.Slot if tx_block.HasField("Header") else -1

        for tx in tx_block.Transactions:
            for instr_update in tx.InstructionBalanceUpdates:
                instr = instr_update.Instruction
                method = instr.Program.Method

                if method not in TARGET_METHODS:
                    continue

                for bal_update in instr_update.TotalCurrencyBalanceUpdates:
                    mint_bytes = bal_update.Currency.MintAddress
                    mint_address = base58.b58encode(mint_bytes).decode().strip()

                    if not mint_address.lower().endswith("bonk"):
                        continue

                    tx_signature = base58.b58encode(tx.Signature).decode().strip()
                    dedup_key = f"{mint_address}:{block_slot}"
                    with dedup_lock:
                        if dedup_key in deduped_keys:
                            return
                        deduped_keys.add(dedup_key)

                    logging.info("✅ New token mint detected")
                    logging.info(f"   - Mint: https://solscan.io/token/{mint_address}")
                    logging.info(f"   - Tx: https://solscan.io/tx/{tx_signature}")
                    logging.info(f"   - Slot: {block_slot}")
                    return
    except DecodeError:
        logging.error("Protobuf decode error", exc_info=True)
    except Exception:
        logging.error("Unexpected error", exc_info=True)

def message_consumer():
    consumer = create_consumer()
    consumer.subscribe([config.KAFKA_TOPIC])
    try:
        while True:
            messages = consumer.consume(num_messages=500, timeout=0.05)
            if not messages:
                continue
            for msg in messages:
                if msg is None or msg.error():
                    continue
                payload = msg.value()
                if not quick_relevance_check(payload):
                    continue
                message_queue.put((payload, None))
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

def message_processor():
    while True:
        try:
            payload, _ = message_queue.get(timeout=1)
            process_kafka_message(payload, None)
        except Empty:
            continue
        except Exception:
            logging.exception("Error in message processor")

def run_bot():
    threading.Thread(target=message_processor, daemon=True).start()
    message_consumer()

if __name__ == "__main__":
    run_bot()
