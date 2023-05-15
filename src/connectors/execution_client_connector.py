from web3 import Web3
from web3.exceptions import NoABIFound
import json
import requests
from enum import Enum
from os import listdir
from connectors.sql_database_connector import SqlDatabaseConnector


timeout = 60

# init token standards
TokenStandard = Enum("TokenStandard", [(token_standard.replace(
    ".json", ""), token_standard.replace(
    ".json", "")) for token_standard in listdir(f"src/token_standard") if token_standard.endswith(".json")])

token_standards = {}
for token_standard in TokenStandard:
    with open(f"src/token_standard/{token_standard.name}.json", "r") as f:
        token_standards[token_standard.name] = json.load(f)


class ExecutionClientConnector:

    def __init__(self,
                 execution_client_url,
                 etherscan_ip: str,
                 etherscan_api_key: str,
                 sql_db_connector: SqlDatabaseConnector,
                 contract_table_name: str
                 ) -> None:
        # execution client params
        self.execution_client_url = execution_client_url
        self.token_standards = token_standards
        # set etherscan api params
        self.etherscan_ip = etherscan_ip
        self.etherscan_api_key = etherscan_api_key
        # set sql db connector
        self.sql_db_connector = sql_db_connector
        self.contract_table_name = contract_table_name
        # init execution client
        self.execution_client = Web3(Web3.HTTPProvider(
            self.execution_client_url, request_kwargs={'timeout': timeout}))

    def get_block_number(self):
        response = self.execution_client.eth.get_block_number()

        return {"block_number": response}

    def get_block(self, block_identifier=None, full_transactions=False):
        if block_identifier is None:
            response = self.execution_client.eth.get_block(
                self.execution_client.eth.default_block, full_transactions)
        else:
            response = self.execution_client.eth.get_block(
                block_identifier, full_transactions)

        return json.loads(Web3.to_json(response))

    def get_transaction(self, transaction_hash: str, decode_input: bool = True):
        response = self.execution_client.eth.get_transaction(
            transaction_hash)

        response = json.loads(Web3.to_json(response))

        if decode_input and response["input"] != "0x":
            contract_address = response["to"]

            contract_abi = self.get_contract_abi(contract_address)

            contract = self.execution_client.eth.contract(
                address=contract_address, abi=contract_abi)

            func_obj, func_params = contract.decode_function_input(
                response["input"])

            for param_name, param in func_params.items():
                if type(param) is int:
                    func_params[param_name] = str(param)
                if type(param) is bytes:
                    func_params[param_name] = f"0x{param.hex()}"

            decoded_input = {
                "function": func_obj,
                "params": func_params
            }

            response["input_decoded"] = decoded_input

        return response

    def get_token_standard_abi(self, token_standard: TokenStandard):
        return self.token_standards[token_standard.name]

    def get_contract_abi(self, contract_address: str):
        """
        Return contract abi.
        Query from db if in db else query from etherscan (no other way)
        """
        if self.sql_db_connector.is_contract_in_db(self.contract_table_name, contract_address):
            contract_data = self.sql_db_connector.query_all_contract_data(
                self.contract_table_name, contract_address)
            return contract_data["abi"]
        else:
            # contract abi can not be retrieved from blockchain (not with get_code()) -> etherscan is needed
            params = {
                "module": "contract",
                "action": "getabi",
                "address":  contract_address,
                "apikey": self.etherscan_api_key
            }
            response = requests.get(self.etherscan_ip, params=params)

            if response.json()["status"] == "0":
                raise NoABIFound

            contract_abi = json.loads(response.json()["result"])

            contract_metadata = self.get_contract_metadata(
                contract_address, contract_abi)

            contract_implemented_token_standards = self.get_contract_implemented_token_standards(
                contract_address, contract_abi)

            # insert data into db
            self.sql_db_connector.insert_contract_data(
                self.contract_table_name, contract_address, contract_metadata, contract_implemented_token_standards, contract_abi)

            return contract_abi

    def get_contract_implemented_token_standards(self, contract_address: str, contract_abi=None):
        # TODO: improve filter function with more than function name
        if contract_abi is None:
            contract_abi = self.get_contract_abi(contract_address)

        contract_abi = [
            contract_function_abi for contract_function_abi in contract_abi if "name" in contract_function_abi.keys()]

        implemented_token_standards = {}

        for token_standard_name, token_standard_abi in self.token_standards.items():
            # implemented flag
            implemented = True

            for token_standard_function_abi in token_standard_abi:
                contract_function_filter = [
                    contract_function for contract_function in contract_abi if contract_function["name"] == token_standard_function_abi["name"]]

                if len(contract_function_filter) == 0:
                    implemented = False
                    break

            implemented_token_standards[token_standard_name] = implemented

        return implemented_token_standards

    def execute_contract_function(self, contract_address: str, function_name: str, contract_abi=None,  *function_args):
        if contract_abi is None:
            contract_abi = self.get_contract_abi(contract_address)

        contract = self.execution_client.eth.contract(
            Web3.to_checksum_address(contract_address), abi=contract_abi)

        contract_function = contract.functions[function_name]

        response = contract_function(*function_args).call()

        return response

    def get_contract_deploy_block(self, contract_address: str):
        try:
            event_filter = self.execution_client.eth.filter({
                "fromBlock": 0,
                "toBlock": "latest",
                "address": contract_address
            })

            response = event_filter.get_all_entries()

            if len(response) == 0:
                return None

            response = response[0]["blockNumber"]

            return response

        except ValueError as e:
            if "data" in e.args[0].keys():
                return int(e.args[0]["data"]["from"], 16)
            else:
                return None

    def get_contract_metadata(self, contract_address: str, contract_abi=None):
        try:
            token_name = self.execute_contract_function(
                contract_address, "name", contract_abi)
        except:
            token_name = None
        try:
            token_symbol = self.execute_contract_function(
                contract_address, "symbol", contract_abi)
        except:
            token_symbol = None
        try:
            deploy_block = self.get_contract_deploy_block(contract_address)
        except:
            deploy_block = None
        try:
            token_total_supply = self.execute_contract_function(
                contract_address, "totalSupply", contract_abi)
        except:
            token_total_supply = None

        return {
            "address": contract_address,
            "name": token_name,
            "symbol": token_symbol,
            "block_deployed": deploy_block,
            "total_supply": token_total_supply
        }
