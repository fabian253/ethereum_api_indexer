import mysql.connector
from mysql.connector import errorcode
import json
from typing import Union


class SqlDatabaseConnector:

    def __init__(self, host: str, port: int, user: str, password: str) -> None:
        config = {
            'user': user,
            'password': password,
            'host': host,
            'port': port
        }

        self.connection = mysql.connector.connect(**config)

    def __del__(self):
        self.connection.close()

    def create_database(self, db_name: str):
        cursor = self.connection.cursor()
        try:
            cursor.execute(
                f"CREATE DATABASE {db_name} DEFAULT CHARACTER SET 'utf8'")
        except mysql.connector.Error as err:
            print(f"Failed creating database: {err}")

        cursor.close()

    def use_database(self, db_name: str):
        cursor = self.connection.cursor()
        try:
            cursor.execute(f"USE {db_name}")
        except mysql.connector.Error as err:
            print(f"Database {db_name} does not exists.")
            if err.errno == errorcode.ER_BAD_DB_ERROR:
                self.create_database(db_name)
                print(f"Database {db_name} created successfully.")
                self.connection.database = db_name
            else:
                print(err)

        cursor.close()

    def create_table(self, table_description: str):
        cursor = self.connection.cursor()
        try:
            print("Creating table: ", end='')
            cursor.execute(f"CREATE TABLE {table_description}")
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print("already exists.")
            else:
                print(err.msg)
        else:
            print("OK")

        cursor.close()

    def insert_data(self, table_name: str, data: dict):
        cursor = self.connection.cursor()
        data_fields = ", ".join(data.keys())
        data_value_slots = ", ".join([f"%({key})s" for key in data.keys()])

        insert_query = f"INSERT INTO {table_name} ({data_fields}) VALUES ({data_value_slots})"

        cursor.execute(insert_query, data)

        self.connection.commit()

        cursor.close()

    def query_data(self, table_name: str, fields: Union[list, str] = "*", equal_filter: dict = None, limit: int = 1000) -> list:
        cursor = self.connection.cursor(dictionary=True, prepared=True)

        if fields == "*":
            data_fields = fields
        else:
            data_fields = ", ".join(fields)

        if equal_filter is None:
            select_query = f"SELECT {data_fields} FROM {table_name} LIMIT {limit}"
        else:
            data_filter_slots = " and ".join(
                [f"{key} = %({key})s" for key in equal_filter.keys()])
            select_query = f"SELECT {data_fields} FROM {table_name} WHERE {data_filter_slots} LIMIT {limit}"

        cursor.execute(select_query, equal_filter)

        data_list = cursor.fetchall()

        cursor.close()

        return data_list

    def query_data_type(self, table_name: str):
        cursor = self.connection.cursor(dictionary=True, prepared=True)

        select_query = f"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS where TABLE_NAME = '{table_name}'"

        cursor.execute(select_query)

        data_type_list = cursor.fetchall()

        data_type_dict = {value["COLUMN_NAME"]: value["DATA_TYPE"]
                          for value in data_type_list}

        cursor.close()

        return data_type_dict

    def insert_contract_data(self, table_name: str, contract_address: str, contract_metadata: dict, block_minted: Union[int, None],  contract_implemented_token_standards: dict, contract_abi):
        data = {
            "contract_address": contract_address,
            "name": contract_metadata["name"],
            "symbol": contract_metadata["symbol"],
            "block_minted": block_minted,
            "total_supply": contract_metadata["total_supply"],
        }

        data.update(contract_implemented_token_standards)

        data["abi"] = json.dumps(contract_abi)

        self.insert_data(table_name, data)

    def query_contract_data(self, table_name: str, contract_address: str) -> dict:
        data = self.query_data(table_name, equal_filter={
                               "contract_address": contract_address})[0]

        data_types = self.query_data_type(table_name)

        # convert tinyint to boolean and decode json
        for column, data_type in data_types.items():
            if data_type == "tinyint":
                data[column] = True if data[column] == 1 else False

            if data_type == "json":
                data[column] = json.loads(data[column].decode("utf-8"))

            if data_type == "varchar" and data[column].isdigit():
                data[column] = int(data[column])

        return data

    def query_erc721_contract_addresses(self, table_name: str, limit: int = 1000) -> list:
        return self.__query_contract_addresses(table_name, "ERC721", limit)

    def query_erc20_contract_addresses(self, table_name: str, limit: int = 1000) -> list:
        return self.__query_contract_addresses(table_name, "ERC20", limit)

    def __query_contract_addresses(self, table_name: str, token_standard: str, limit: int = 1000) -> list:
        data = self.query_data(
            table_name, ["contract_address"], {token_standard: True}, limit)

        data = [contract["contract_address"] for contract in data]

        return data

    def is_contract_in_db(self, table_name: str, contract_address: str) -> bool:
        contract = self.query_data(table_name, equal_filter={
                                   "contract_address": contract_address})

        if len(contract) == 0:
            return False
        else:
            return True
