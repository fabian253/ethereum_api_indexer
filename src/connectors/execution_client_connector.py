from web3 import Web3
from web3.exceptions import BlockNotFound
import json
from enum import Enum
from os import listdir


TokenStandard = Enum("TokenStandard", [(token_standard.replace(
    ".json", ""), token_standard.replace(
    ".json", "")) for token_standard in listdir(f"src/token_standard")])


class ExecutionClientConnector:

    def __init__(self,
                 execution_client_url,
                 etherscan_ip: str,
                 etherscan_api_key: str,
                 token_standards: dict
                 ) -> None:
        self.etherscan_ip = etherscan_ip
        self.etherscan_api_key = etherscan_api_key
        self.token_standards = token_standards
        # init execution client
        self.execution_client = Web3(Web3.HTTPProvider(execution_client_url))

    def get_block_number(self):
        response = self.execution_client.eth.get_block_number()

        return {"block_number": response}

    def get_block(self, block_identifier=None, full_transactions=False):
        try:
            if block_identifier is None:
                response = self.execution_client.eth.get_block(
                    self.execution_client.eth.default_block, full_transactions)
            else:
                response = self.execution_client.eth.get_block(
                    block_identifier, full_transactions)
        except (BlockNotFound, ValueError):
            return None

        return json.loads(Web3.toJSON(response))

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
            Web3.toChecksumAddress(contract_address), abi=contract_abi)

        contract_function = contract.functions[function_name]

        response = contract_function(*function_args).call()

        return response

    def get_contract_mint_block(self, contract_address: str):
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
            return int(e.args[0]["data"]["from"], 16)

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
            token_total_supply = self.execute_contract_function(
                contract_address, "totalSupply", contract_abi)
        except:
            token_total_supply = None

        return {
            "address": contract_address,
            "name": token_name,
            "symbol": token_symbol,
            "total_supply": token_total_supply
        }
