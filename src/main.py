from connectors.sql_database_connector import SqlDatabaseConnector
from connectors.etherscan_connector import EtherscanConnector
from connectors.execution_client_connector import ExecutionClientConnector, TokenStandard
import db_metadata.sql_tables as tables
import config
import json


# init sql database connector
sql_db_connector = SqlDatabaseConnector(
    config.SQL_DATABASE_HOST,
    config.SQL_DATABASE_PORT,
    config.SQL_DATABASE_USER,
    config.SQL_DATABASE_PASSWORD
)
sql_db_connector.use_database(config.SQL_DATABASE_NAME)
sql_db_connector.create_table(tables.CONTRACT_TABLE)


execution_client_url = f"http://{config.EXECUTION_CLIENT_IP}:{config.EXECUTION_CLIENT_PORT}"

# init token standards
token_standards = {}
for token_standard in TokenStandard:
    with open(f"src/token_standard/{token_standard.name}.json", "r") as f:
        token_standards[token_standard.name] = json.load(f)

execution_client = ExecutionClientConnector(
    execution_client_url, config.ETHERSCAN_URL, config.ETHERSCAN_API_KEY, token_standards)


# TODO: remove when node is fully synced -> currently used for contract endpoints only
infura_execution_client_url = f"{config.INFURA_URL}/{config.INFURA_API_KEY}"
infura_execution_client = ExecutionClientConnector(
    infura_execution_client_url, config.ETHERSCAN_URL, config.ETHERSCAN_API_KEY, token_standards)

# init connectors

etherscan_connector = EtherscanConnector(
    config.ETHERSCAN_URL, config.ETHERSCAN_API_KEY)

if __name__ == "__main__":
    block_count = config.INDEXING_END_BLOCK+1 - config.INDEXING_START_BLOCK

    for block_idx, block_identifier in enumerate(range(config.INDEXING_START_BLOCK, config.INDEXING_END_BLOCK+1)):

        block = execution_client.get_block(block_identifier, True)

        block_transactions = [
            transaction for transaction in block["transactions"] if transaction["input"] != "0x"]

        contract_addresses = [address["to"] for address in block_transactions]

        contract_addresses = list(dict.fromkeys(contract_addresses))

        inserted_contract_counter = 0

        for contract_address in contract_addresses:

            if not sql_db_connector.is_contract_in_db(config.SQL_DATABASE_TABLE_CONTRACT, contract_address):

                contract_abi = etherscan_connector.get_contract_abi(
                    contract_address)

                if contract_abi is not None:

                    contract_metadata = infura_execution_client.get_contract_metadata(
                        contract_address, contract_abi)

                    contract_implemented_token_standards = infura_execution_client.get_contract_implemented_token_standards(
                        contract_address, contract_abi)

                    try:
                        sql_db_connector.insert_contract_data(
                            config.SQL_DATABASE_TABLE_CONTRACT, contract_address, contract_metadata, contract_implemented_token_standards, contract_abi)

                        inserted_contract_counter += 1
                    except:
                        print(
                            f"Contract {contract_address} error while inserting.")

                    print(f"Contract {contract_address} inserted.")

        print(
            f"Block {block_identifier} done, inserted {inserted_contract_counter} contracts. [{block_idx+1}/{block_count}]")