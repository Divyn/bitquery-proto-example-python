# Pump.fun migrate program ID
PUMPFUN_MIGRATE_PROGRAM_ID = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P'

def process_message(message):
    global decode_errors, processed_count
    try:
        buffer = message.value()
        tx_block = parsed_idl_block_message_pb2.ParsedIdlBlockMessage()
        tx_block.ParseFromString(buffer)

        for tx in tx_block.Transactions:
            processed_count += 1
            tx_logs = []
            found_migrate = False

            for instruction in tx.ParsedIdlInstructions:
                # collect logs
                if instruction.Logs:
                    # tx_logs.extend(instruction.Logs) save logs for all tx if needed
                    
                    # Check if this instruction is from Pump.fun migrate program
                    if base58.b58encode(instruction.Program.Address).decode() == PUMPFUN_MIGRATE_PROGRAM_ID:
                        for log in instruction.Logs:
                            if "Instruction: Migrate" in log:
                                found_migrate = True

            if found_migrate:
                # Further logic based on your usecase
                

    except DecodeError as err:
        decode_errors += 1
        if decode_errors % 100 == 0:
            print(f"Total decode errors so far: {decode_errors}")
    except Exception as err:
        print(f"Error processing message: {err}")
