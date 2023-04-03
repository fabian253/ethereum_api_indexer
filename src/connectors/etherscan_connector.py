import requests
import json


class EtherscanConnector:

    def __init__(self, etherscan_ip: str, etherscan_api_key: str) -> None:
        self.client_url = etherscan_ip
        self.api_key = etherscan_api_key

    def get_block(self, block_identifier, full_transactions=False):
        # type conveserion for hex
        if type(block_identifier) is str:
            block_identifier = int(block_identifier, 0)

        params = {
            "module": "proxy",
            "action": "eth_getBlockByNumber",
            "tag":  hex(block_identifier),
            "boolean": full_transactions,
            "apikey": self.api_key
        }

        response = requests.get(self.client_url, params=params)

        return response.json()

    def get_transaction(self, transaction_hash: str):
        params = {
            "module": "proxy",
            "action": "eth_getTransactionByHash",
            "txhash":  transaction_hash,
            "apikey": self.api_key
        }

        response = requests.get(self.client_url, params=params)

        return response.json()

    def get_block_transaction_count(self, block_identifier):
        # type conveserion for hex
        if type(block_identifier) is str:
            block_identifier = int(block_identifier, 0)

        params = {
            "module": "proxy",
            "action": "eth_getBlockTransactionCountByNumber",
            "tag":  hex(block_identifier),
            "apikey": self.api_key
        }

        response = requests.get(self.client_url, params=params)

        return response.json()

    def get_wallet_balance(self, wallet_address, block_identifier="latest"):
        params = {
            "module": "account",
            "action": "balance",
            "address": wallet_address,
            "tag":  block_identifier,
            "apikey": self.api_key
        }

        response = requests.get(self.client_url, params=params)

        return response.json()

    def get_contract_abi(self, contract_address: str):
        params = {
            "module": "contract",
            "action": "getabi",
            "address":  contract_address,
            "apikey": self.api_key
        }

        response = requests.get(self.client_url, params=params)

        if response.json()["status"] == "0":
            return None

        contract_abi = json.loads(response.json()["result"])

        return contract_abi
