import mysql.connector
from mysql.connector import errorcode
from mysql.connector.errors import DatabaseError
import json
from typing import Union
import logging


class SqlDatabaseConnector:

    def __init__(self, host: str, port: int, user: str, password: str, db_name: str, tables: list = None, with_logging: bool = True) -> None:
        # set logging
        if with_logging:
            logging.basicConfig(level=logging.INFO)
        else:
            logging.basicConfig(level=logging.ERROR)

        self.config = {
            'user': user,
            'password': password,
            'host': host,
            'port': port
        }
        self.db_name = db_name

        self.connection = None
        try:
            self.connect(self.config)

            self.db_name = db_name
            self.use_database(db_name)

            for table in tables:
                self.create_table(table)
        except DatabaseError:
            pass

    def __del__(self):
        if self.connection is not None:
            self.connection.close()

    # General Functions

    def connect(self, config: Union[dict, None] = None):
        try:
            if config is not None:
                self.connection = mysql.connector.connect(**config)
                logging.info("SQL server connected")
            elif self.connection is None or not self.connection.is_connected():
                self.connection = mysql.connector.connect(**self.config)
                logging.info("SQL server connected")
        except DatabaseError:
            raise DatabaseError("Database not available")

    def create_database(self, db_name: str):
        self.connect()

        cursor = self.connection.cursor()
        try:
            cursor.execute(
                f"CREATE DATABASE {db_name} DEFAULT CHARACTER SET 'utf8'")
            logging.info(f"Created database: {db_name}")
        except mysql.connector.Error as err:
            logging.error(f"Failed creating database: {db_name}, error: {err}")

        cursor.close()

    def use_database(self, db_name: str):
        self.connect()

        db_in_use = self.get_database_in_use()

        if db_in_use is None or db_in_use != db_name:
            cursor = self.connection.cursor()
            try:
                cursor.execute(f"USE {db_name}")
                logging.info(f"Using database: {db_name}")
            except mysql.connector.Error as err:
                logging.error(f"Database does not exist: {db_name}")
                if err.errno == errorcode.ER_BAD_DB_ERROR:
                    self.create_database(db_name)
                    self.connection.db_name = db_name
                else:
                    print(err)

            cursor.close()

    def get_database_in_use(self) -> str:
        self.connect()

        cursor = self.connection.cursor()
        cursor.execute("select database()")
        database = cursor.fetchone()[0]
        cursor.close()

        return database

    def create_table(self, table_description: str):
        self.use_database(self.db_name)

        table_name = table_description.split(" ", 1)[0]

        cursor = self.connection.cursor()
        try:
            cursor.execute(f"CREATE TABLE {table_description}")
            logging.info(f"Created table: {table_name}")
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                logging.info(f"Created table already exists: {table_name}")
            else:
                logging.error(
                    f"Failed creating table: {table_name}, error: {err.msg}")

        cursor.close()

    def insert_data(self, table_name: str, data: dict):
        self.use_database(self.db_name)

        data_fields = ", ".join(data.keys())
        data_value_slots = ", ".join([f"%({key})s" for key in data.keys()])

        insert_query = f"INSERT INTO {table_name} ({data_fields}) VALUES ({data_value_slots})"

        cursor = self.connection.cursor()
        cursor.execute(insert_query, data)
        self.connection.commit()
        cursor.close()

    def insert_many_data(self, table_name: str, many_data: list[dict], batch_size: int = 10000):
        self.use_database(self.db_name)

        field_names = many_data[0].keys()
        data_fields = ", ".join(field_names)
        data_value_slots = ", ".join([f"%({key})s" for key in field_names])

        insert_query = f"INSERT IGNORE INTO {table_name} ({data_fields}) VALUES ({data_value_slots})"

        cursor = self.connection.cursor()

        if len(many_data) > batch_size:
            batch_start = 0
            batch_end = batch_size

            while batch_end <= len(many_data)+1:
                batch = many_data[batch_start:batch_end]

                cursor.executemany(insert_query, batch)
                self.connection.commit()

                # calc new batch
                batch_start = batch_end
                if batch_start >= len(many_data):
                    break
                elif batch_start + batch_start <= len(many_data):
                    batch_end = batch_start + batch_size
                else:
                    batch_end = len(many_data)+1
        else:
            cursor.executemany(insert_query, many_data)
            self.connection.commit()

        cursor.close()

    def query_data(self, table_name: str, fields: Union[list, str] = "*", equal_filter: dict = None, limit: int = 1000) -> list:
        self.use_database(self.db_name)

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

        cursor = self.connection.cursor(dictionary=True, prepared=True)
        cursor.execute(select_query, equal_filter)
        data_list = cursor.fetchall()
        cursor.close()

        return data_list

    def query_data_type(self, table_name: str):
        self.use_database(self.db_name)

        select_query = f"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS where TABLE_NAME = '{table_name}'"

        cursor = self.connection.cursor(dictionary=True, prepared=True)
        cursor.execute(select_query)
        data_type_list = cursor.fetchall()
        data_type_dict = {value["COLUMN_NAME"]: value["DATA_TYPE"]
                          for value in data_type_list}
        cursor.close()

        return data_type_dict

    # Contract Functions

    def insert_contract_data(self, table_name: str, contract_address: str, contract_metadata: dict, contract_implemented_token_standards: dict, contract_abi):
        data = {
            "contract_address": contract_address,
            "name": contract_metadata["name"],
            "symbol": contract_metadata["symbol"],
            "block_deployed": contract_metadata["block_deployed"],
            "total_supply": contract_metadata["total_supply"],
        }

        data.update(contract_implemented_token_standards)

        data["abi"] = json.dumps(contract_abi)

        self.insert_data(table_name, data)

    def query_all_contract_data(self, table_name: str, contract_address: str) -> dict:
        data = self.query_data(table_name, equal_filter={
                               "contract_address": contract_address})[0]

        data_types = self.query_data_type(table_name)

        # convert tinyint to boolean and decode json
        for column, data_type in data_types.items():
            if data[column] is not None:
                if data_type == "tinyint":
                    data[column] = True if data[column] == 1 else False

                if data_type == "json":
                    data[column] = json.loads(data[column])

                if data_type == "varchar" and data[column].isdigit():
                    data[column] = int(data[column])

        return data

    def query_contract_data(self,
                            table_name: str,
                            token_standard: Union[str, None] = None,
                            with_name: bool = False,
                            with_symbol: bool = False,
                            with_block_deployed: bool = False,
                            with_total_supply: bool = False,
                            with_abi: bool = False,
                            limit: int = 1000) -> list:
        fields = ["contract_address"]
        if with_name:
            fields.append("name")
        if with_symbol:
            fields.append("symbol")
        if with_block_deployed:
            fields.append("block_deployed")
        if with_total_supply:
            fields.append("total_supply")
        if with_abi:
            fields.append("abi")

        if token_standard is None:
            data = self.query_data(
                table_name, fields, None, limit)
        else:
            data = self.query_data(
                table_name, fields, {token_standard: True}, limit)

        data_types = self.query_data_type(table_name)

        for d in data:
            for key, value in d.items():
                if value is not None:
                    if data_types[key] == "json":
                        d[key] = json.loads(value.decode("utf-8"))

                    if data_types[key] == "varchar" and value.isdigit():
                        d[key] = int(value)

        return data

    def is_contract_in_db(self, table_name: str, contract_address: str) -> bool:
        contract = self.query_data(table_name, equal_filter={
                                   "contract_address": contract_address})

        if len(contract) == 0:
            return False
        else:
            return True

    # Transaction Functions

    def insert_transaction_data(self, table_name: str, transaction_hash: str, contract_address: str, token_id: int, from_address: str, to_address: str, block_number: int):
        if not self.is_transaction_in_db(table_name, transaction_hash):
            data = {
                "transaction_hash": transaction_hash,
                "contract_address": contract_address,
                "token_id": token_id,
                "from_address": from_address,
                "to_address": to_address,
                "block_number": block_number
            }

            self.insert_data(table_name, data)

    def insert_many_transaction_data(self, table_name: str, transaction_data: list[dict]):
        self.insert_many_data(table_name, transaction_data)

    def query_contract_transaction_data(self, table_name: str, contract_address: str, token_id: Union[int, None] = None, fields: Union[list, str] = "*"):
        filters = {
            "contract_address": contract_address
        }
        if token_id is not None:
            filters["token_id"] = token_id

        contract_transactions = self.query_data(
            table_name, fields, filters, limit=1000000000)

        #response = sorted(response, key=lambda x: x["token_id"])

        return contract_transactions

    def query_token_transaction_data(self, table_name: str, contract_address: str, token_id: int):
        # return list of transactions sorted by block_number
        fields = ["from_address", "to_address", "block_number"]

        token_transactions = self.query_contract_transaction_data(
            table_name, contract_address, token_id, fields)
        token_transactions = sorted(
            token_transactions, key=lambda x: x["block_number"])

        return token_transactions

    def is_transaction_in_db(self, table_name: str, transaction_hash: str) -> bool:
        contract = self.query_data(table_name, equal_filter={
                                   "transaction_hash": transaction_hash})

        if len(contract) == 0:
            return False
        else:
            return True

    def update_contract_deploy_block(self, table_name: str, contract_address: str, block_deployed: int):
        self.use_database(self.db_name)

        insert_query = f"UPDATE {table_name} SET block_deployed={block_deployed} WHERE contract_address='{contract_address}'"

        cursor = self.connection.cursor()
        cursor.execute(insert_query)
        self.connection.commit()
        cursor.close()
