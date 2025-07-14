// This code snippet tracks BSC Mempool Transactions
// Import required modules
const { Kafka } = require("kafkajs");
const bs58 = require("bs58");
const { loadProto } = require("bitquery-protobuf-schema");
const { CompressionTypes, CompressionCodecs } = require("kafkajs");
const LZ4 = require("kafkajs-lz4");
const { v4: uuidv4 } = require("uuid");
const config = require("./config.json"); //credentials imported from JSON file
// Enable LZ4 compression
CompressionCodecs[CompressionTypes.LZ4] = new LZ4().codec;

// Configuration
const username = config.bsc_username; //credentials imported from JSON file
const password = config.bsc_password;
const topic = "bsc.broadcasted.transactions.proto";
const id = uuidv4();

// Initialize Kafka Client (Non-SSL)
const kafka = new Kafka({
  clientId: username,
  brokers: [
    "rpk0.bitquery.io:9092",
    "rpk1.bitquery.io:9092",
    "rpk2.bitquery.io:9092",
  ],
  sasl: {
    mechanism: "scram-sha-512",
    username: username,
    password: password,
  },
});

// Function to convert bytes to base58
const convertBytes = (buffer, encoding = "base58") => {
  if (encoding === "base58") {
    return bs58.default.encode(buffer);
  }
  return buffer.toString("hex");
};

// Recursive function to print Protobuf messages
const printProtobufMessage = (msg, indent = 0, encoding = "base58") => {
  const prefix = " ".repeat(indent);
  for (const [key, value] of Object.entries(msg)) {
    if (Array.isArray(value)) {
      console.log(`${prefix}${key} (repeated):`);
      value.forEach((item, idx) => {
        if (typeof item === "object" && item !== null) {
          console.log(`${prefix}  [${idx}]:`);
          printProtobufMessage(item, indent + 4, encoding);
        } else {
          console.log(`${prefix}  [${idx}]: ${item}`);
        }
      });
    } else if (value && typeof value === "object" && Buffer.isBuffer(value)) {
      console.log(`${prefix}${key}: ${convertBytes(value, encoding)}`);
    } else if (value && typeof value === "object") {
      console.log(`${prefix}${key}:`);
      printProtobufMessage(value, indent + 4, encoding);
    } else {
      console.log(`${prefix}${key}: ${value}`);
    }
  }
};

// Initialize consumer
const consumer = kafka.consumer({ groupId: `${username}-${id}` });

// Run the consumer
const run = async () => {
  try {
    const MessageToParse = await loadProto(topic); // Load Protobuf schema
    await consumer.connect();
    await consumer.subscribe({ topic, fromBeginning: false });

    await consumer.run({
      autoCommit: false,
      eachMessage: async ({ partition, message }) => {
        try {
          const buffer = message.value;
          const decoded = MessageToParse.decode(buffer);
          const msgObj = MessageToParse.toObject(decoded, {
            bytes: Buffer,
          });
          printProtobufMessage(msgObj);
        } catch (err) {
          console.error("Error decoding Protobuf message:", err);
        }
      },
    });
  } catch (error) {
    console.error("Error running consumer:", error);
  }
};

// Start the consumer
run().catch(console.error);
