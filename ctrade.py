"""
Solana DEX trade filter (optimized)

Key features:
- Manual partition assignment (no group/rebalances).
- Batching on both Kafka fetch and processing.
- Bytes-to-bytes compare for TARGET_PROGRAM (no base58 in hot path).
- Protobuf object reuse to reduce allocations.
- Bounded queue with "drop-oldest" backpressure.
- Graceful shutdown via stop_event.
"""

import uuid
import base58
import threading
import time
import queue
import logging
import signal
from typing import List

from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition
from google.protobuf.message import DecodeError
from google.protobuf.descriptor import FieldDescriptor

from solana import dex_block_message_pb2

import config

# ---------------------- Constants & Config ----------------------
group_id_suffix = uuid.uuid4().hex
TOPIC = "solana.dextrades.proto"
NUM_PARTITIONS = 6                    # fixed, as per user
NUM_CONSUMERS = NUM_PARTITIONS        # one thread per partition

# The BaseCurrency we are checking for (Base58 format)
TARGET_BASE_CURRENCY = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
TARGET_BASE_CURRENCY_BYTES = base58.b58decode(TARGET_BASE_CURRENCY)
SOLANA_SYSTEM_PROGRAM = "So11111111111111111111111111111111111111112"  # Ignore this

# Kafka client config
BASE_CONF = {
    "bootstrap.servers": "rpk0.bitquery.io:9092,rpk1.bitquery.io:9092,rpk2.bitquery.io:9092",
    "session.timeout.ms": 30000,
    "security.protocol": "SASL_PLAINTEXT",
    "ssl.endpoint.identification.algorithm": "none",
    "sasl.mechanisms": "SCRAM-SHA-512",
    "sasl.username": config.solana_username,
    "sasl.password": config.solana_password,
    "auto.offset.reset": "latest",
    "enable.auto.commit": False,  
    "group.id": f"{config.solana_username}-group-{group_id_suffix}",

    "fetch.min.bytes": 1_048_576,     # 1 MiB
    "fetch.wait.max.ms": 50,          # allow broker to coalesce
    "queued.min.messages": 100_000,
    "enable.partition.eof": False,
}

# Queue for batches from all consumers -> processor
BATCH_QUEUE: "queue.Queue[List]" = queue.Queue(maxsize=2000)

# Shutdown coordination
stop_event = threading.Event()

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(threadName)s]: %(message)s",
)

# ---------------------- Prints Entire Message ----------------------

def print_protobuf_message(msg, indent=0, encoding="base58"):
    """Debug helper to dump any protobuf message (optional; not used in hot path)."""
    prefix = " " * indent
    for field in msg.DESCRIPTOR.fields:
        value = getattr(msg, field.name)
        if field.label == FieldDescriptor.LABEL_REPEATED:
            if not value:
                continue
            print(f"{prefix}{field.name} (repeated):")
            for idx, item in enumerate(value):
                if field.type == FieldDescriptor.TYPE_MESSAGE:
                    print(f"{prefix}  [{idx}]:")
                    print_protobuf_message(item, indent + 4, encoding)
                elif field.type == FieldDescriptor.TYPE_BYTES:
                    if encoding == "base58":
                        s = base58.b58encode(item).decode()
                    else:
                        s = item.hex()
                    print(f"{prefix}  [{idx}]: {s}")
                else:
                    print(f"{prefix}  [{idx}]: {item}")

        elif field.type == FieldDescriptor.TYPE_MESSAGE:
            if msg.HasField(field.name):
                print(f"{prefix}{field.name}:")
                print_protobuf_message(value, indent + 4, encoding)

        elif field.type == FieldDescriptor.TYPE_BYTES:
            s = base58.b58encode(value).decode() if encoding == "base58" else value.hex()
            print(f"{prefix}{field.name}: {s}")

        elif field.containing_oneof:
            if msg.WhichOneof(field.containing_oneof.name) == field.name:
                print(f"{prefix}{field.name} (oneof): {value}")

        else:
            print(f"{prefix}{field.name}: {value}")


# ---------------------- Matching & Processing ----------------------

def transaction_matches_target(tx) -> bool:
    """Return True if any trade in tx targets the base currency (bytes compare)."""
    for trade in tx.Trades:
        if trade.HasField("Market") and trade.Market.HasField("BaseCurrency"):
            if trade.Market.BaseCurrency.MintAddress == TARGET_BASE_CURRENCY_BYTES:
                return True
    return False


def process_batch(batch_msgs):
    """
    Parse and handle a batch of Kafka messages.
    Reuses the protobuf object to reduce allocations.
    """
    tx_block = dex_block_message_pb2.DexParsedBlockMessage()

    for message in batch_msgs:
        if message is None:
            continue
        try:
            buf = message.value()
            if not buf:
                continue

            tx_block.Clear()
            tx_block.ParseFromString(buf)

            # Iterate transactions; log any matches
            for tx in tx_block.Transactions:
                if transaction_matches_target(tx):
                    sig_b58 = base58.b58encode(tx.Signature).decode()
                    logging.info("Matching Transaction Found!")
                    logging.info("Transaction Signature: %s", sig_b58)
                    
                    # Find and log the matching trade details
                    for trade in tx.Trades:
                        if trade.HasField("Market") and trade.Market.HasField("BaseCurrency"):
                            if trade.Market.BaseCurrency.MintAddress == TARGET_BASE_CURRENCY_BYTES:
                                base_currency_address = base58.b58encode(trade.Market.BaseCurrency.MintAddress).decode()
                                logging.info("BaseCurrency: %s", base_currency_address)
                                logging.info("Trade Amount: %s", trade.Buy.Amount)
                                # print_protobuf_message(trade, encoding='base58') #uncomment this line to get full details
                                break

        except DecodeError as err:
            logging.warning("Protobuf decoding error: %s", err)
        except Exception as err:
            logging.exception("Error processing message: %s", err)


# ---------------------- Consumer Worker ----------------------

def consumer_worker(partition_id: int, batch_size: int = 100, poll_timeout: float = 1.0):
    """
    One thread per partition. Manual assignment → no subscribe()/group.
    Fetches messages in batches and enqueues them to the processor.
    """
    conf = dict(BASE_CONF)  # shallow copy
    consumer = Consumer(conf)

    tp = TopicPartition(TOPIC, partition_id)
    consumer.assign([tp])

    logging.info("Consumer pinned to partition %d started", partition_id)

    try:
        while not stop_event.is_set():
            msgs = consumer.consume(num_messages=batch_size, timeout=poll_timeout)
            if not msgs:
                continue

            # bounded queue with drop-oldest policy to maintain forward progress
            try:
                BATCH_QUEUE.put(msgs, timeout=0.5)
            except queue.Full:
                dropped = False
                try:
                    old = BATCH_QUEUE.get_nowait()
                    BATCH_QUEUE.task_done()
                    dropped = True
                except queue.Empty:
                    pass
                try:
                    BATCH_QUEUE.put_nowait(msgs)
                except queue.Full:
                    # If still full, skip this batch
                    pass
                if dropped:
                    logging.warning("Queue full: dropped oldest batch (partition %d)", partition_id)

    except KeyboardInterrupt:
        pass
    except Exception:
        logging.exception("Consumer error on partition %d", partition_id)
    finally:
        try:
            consumer.close()
        except Exception:
            pass
        logging.info("Consumer for partition %d closed", partition_id)


# ---------------------- Processor Thread ----------------------

def message_processor():
    logging.info("Message processor started")
    while not stop_event.is_set():
        try:
            batch = BATCH_QUEUE.get(timeout=0.5)
        except queue.Empty:
            continue
        try:
            process_batch(batch)
        finally:
            BATCH_QUEUE.task_done()
    logging.info("Message processor stopping")


# ---------------------- Main ----------------------

def _install_signal_handlers():
    def _handle(sig, frame):
        logging.info("Signal %s received, shutting down…", sig)
        stop_event.set()
    for s in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(s, _handle)
        except Exception:
            pass


def main():
    _install_signal_handlers()

    processor_thread = threading.Thread(target=message_processor, name="processor", daemon=True)
    processor_thread.start()

    # Start one consumer per partition (0..5)
    consumer_threads = []
    for p in range(NUM_CONSUMERS):
        t = threading.Thread(target=consumer_worker, args=(p,), name=f"consumer-{p}", daemon=True)
        t.start()
        consumer_threads.append(t)

    # Run until stop_event is set (by signal)
    try:
        while not stop_event.is_set():
            time.sleep(1.0)
    finally:

        try:
            BATCH_QUEUE.join()
        except Exception:
            pass

        # Join threads
        for t in consumer_threads:
            t.join(timeout=5)
        processor_thread.join(timeout=5)

    logging.info("Shutdown complete.")


if __name__ == "__main__":
    main()
