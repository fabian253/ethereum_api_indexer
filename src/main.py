from connectors import SqlDatabaseConnector, ExecutionClientConnector, EtherscanConnector
import config
import db_params.sql_tables as tables
from web3.exceptions import NoABIFound
from mysql.connector.errors import DatabaseError
import logging

logging.basicConfig(level=logging.INFO)

# init sql database connector
sql_db_connector = SqlDatabaseConnector(
    config.SQL_DATABASE_HOST,
    config.SQL_DATABASE_PORT,
    config.SQL_DATABASE_USER,
    config.SQL_DATABASE_PASSWORD,
    config.SQL_DATABASE_NAME,
    [tables.CONTRACT_TABLE, tables.TRANSACTION_TABLE]
)

# init execution client
execution_client_url = f"http://{config.EXECUTION_CLIENT_IP}:{config.EXECUTION_CLIENT_PORT}"
execution_client = ExecutionClientConnector(
    execution_client_url, config.ETHERSCAN_URL, config.ETHERSCAN_API_KEY, sql_db_connector, config.SQL_DATABASE_TABLE_CONTRACT)

# TODO: remove when node is fully synced -> currently used for contract endpoints only
# init infura execution client
infura_execution_client_url = f"{config.INFURA_URL}/{config.INFURA_API_KEY}"
infura_execution_client = ExecutionClientConnector(
    infura_execution_client_url, config.ETHERSCAN_URL, config.ETHERSCAN_API_KEY, sql_db_connector, config.SQL_DATABASE_TABLE_CONTRACT)

# init etherscan connector
etherscan_connector = EtherscanConnector(
    config.ETHERSCAN_URL, config.ETHERSCAN_API_KEY)


def process_contract(contract_address: str) -> bool:
    # check if contract is already in db
    if not sql_db_connector.is_contract_in_db(config.SQL_DATABASE_TABLE_CONTRACT, contract_address):
        # try to query contract data
        try:
            contract_abi = etherscan_connector.get_contract_abi(
                contract_address)

            contract_metadata = infura_execution_client.get_contract_metadata(
                contract_address, contract_abi)

            contract_implemented_token_standards = infura_execution_client.get_contract_implemented_token_standards(
                contract_address, contract_abi)

            sql_db_connector.insert_contract_data(config.SQL_DATABASE_TABLE_CONTRACT,
                                                  contract_address,
                                                  contract_metadata,
                                                  contract_implemented_token_standards,
                                                  contract_abi)

            logging.info(f"Contract inserted: {contract_address}")
            return True
        except ConnectionError:
            logging.error(
                f"Inserting error: {contract_address} -> Node not available")
            return False
        except DatabaseError:
            logging.error(
                f"Inserting error: {contract_address} -> Database not available")
            return False
        except NoABIFound:
            logging.error(
                f"Inserting error: {contract_address} -> ABI not found")
            return False
        except:
            logging.error(
                f"Inserting error: {contract_address} -> Unknown error")
            return False
    else:
        return False


def process_block(block_identifier: int) -> int:
    # get block and block transactions
    block = execution_client.get_block(block_identifier, True)
    block_transactions = [
        transaction for transaction in block["transactions"] if transaction["input"] != "0x"]
    # get unique contract addresses from block
    contract_addresses = [address["to"] for address in block_transactions]
    contract_addresses = list(dict.fromkeys(contract_addresses))

    inserted_contract_counter = 0
    # process contracts
    for contract_address in contract_addresses:
        process_result = process_contract(contract_address)
        if process_result:
            inserted_contract_counter += 1

    return inserted_contract_counter


if __name__ == "__main__":
    # create file if it not exists
    f = open("src/process_data/processed_blocks.txt", "a+")
    f.close()
    # read block params
    with open("src/process_data/processed_blocks.txt", "r") as f:
        processed_blocks = f.read()

    processed_blocks = [int(block) for block in processed_blocks.splitlines()]
    block_identifiers = range(
        config.INDEXING_START_BLOCK, config.INDEXING_END_BLOCK)
    block_identifiers = [
        block for block in block_identifiers if not block in processed_blocks]

    block_count = len(block_identifiers)

    # iterate over unindexed blocks
    for block_idx, block_identifier in enumerate(block_identifiers):
        try:
            # process block
            process_result = process_block(block_identifier)

            with open("src/process_data/processed_blocks.txt", "a") as f:
                f.write(f"{block_identifier}\n")

            logging.info(
                f"Block processed: {block_identifier} [{block_idx+1}/{block_count}] -> {process_result} contracts inserted")
        except Exception as e:
            logging.error(
                f"Block processing error: {block_identifier} -> Unknown error")
