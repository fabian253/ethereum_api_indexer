from web3.exceptions import NoABIFound
import requests
import json


class EtherscanConnector:

    def __init__(self, etherscan_ip: str, etherscan_api_key: str) -> None:
        self.client_url = etherscan_ip
        self.api_key = etherscan_api_key

    def get_contract_abi(self, contract_address: str):
        params = {
            "module": "contract",
            "action": "getabi",
            "address":  contract_address,
            "apikey": self.api_key
        }

        response = requests.get(self.client_url, params=params)

        if response.json()["status"] == "0":
            raise NoABIFound

        contract_abi = json.loads(response.json()["result"])

        return contract_abi
