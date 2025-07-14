import json
import os
from confluent_kafka import Consumer, KafkaError, KafkaException
import uuid
from google.protobuf.message import DecodeError
from solana import parsed_idl_block_message_pb2, token_block_message_pb2
import base58
import config

group_id_suffix = uuid.uuid4().hex

# Kafka consumer configuration
conf = {
    'bootstrap.servers': 'rpk0.bitquery.io:9092,rpk1.bitquery.io:9092,rpk2.bitquery.io:9092',
    'group.id': f'{config.solana_username}-group-{group_id_suffix}',  
    'session.timeout.ms': 30000,
    'security.protocol': 'SASL_PLAINTEXT',
    'ssl.endpoint.identification.algorithm': 'none',
    'sasl.mechanisms': 'SCRAM-SHA-512',
    'sasl.username': config.solana_username,
    'sasl.password': config.solana_password,
    'auto.offset.reset': 'latest',
}

# Initialize Kafka consumer
consumer = Consumer(conf)
topic = 'solana.tokens.proto'

# Target program address to filter transactions
TARGET_PROGRAM_ADDRESS = "LanMV9sAd7wArD4vJFi2qDdfnVhFxYSUg6eADduJ3uj"

# Target method
TARGET_METHOD = "initialize"

def process_message(message):
    try:
        buffer = message.value()
        tx_block = token_block_message_pb2.TokenBlockMessage()
        tx_block.ParseFromString(buffer)

        print("\nNew Block Message Received")

        # Block Header
        if tx_block.HasField("Header"):
            header = tx_block.Header
            print(f"Block Slot: {header.Slot}")
            print(f"Block Hash: {base58.b58encode(header.Hash).decode()}")
            print(f"Timestamp: {header.Timestamp}")

        for tx in tx_block.Transactions:
            include_transaction = False

            for instr_update in tx.InstructionBalanceUpdates:
                instr = instr_update.Instruction
                if not instr.HasField("Program"):
                    continue

                program = instr.Program
                program_address = base58.b58encode(program.Address).decode()
                method = program.Method

                if (
                    program_address ==TARGET_PROGRAM_ADDRESS
                    and method == TARGET_METHOD
                ):
                    for bal_update in instr_update.TotalCurrencyBalanceUpdates:
                        mint_address = base58.b58encode(bal_update.Currency.MintAddress).decode()
                        if mint_address.endswith("bonk"):
                            include_transaction = True
                            break
                if include_transaction:
                    break

            if include_transaction:
                print("\nMatching Transaction Details:")
                print(f"Transaction Signature: {base58.b58encode(tx.Signature).decode()}")
                print(f"Transaction Index: {tx.Index}")

    except DecodeError as err:
        print(f"Protobuf decoding error: {err}")
    except Exception as err:
        print(f"Error processing message: {err}")

# Subscribe to the topic
consumer.subscribe([topic])

# Poll messages and process them
try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                raise KafkaException(msg.error())
        process_message(msg)

except KeyboardInterrupt:
    pass

finally:
    consumer.close()
