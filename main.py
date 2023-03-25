try:
    import os
    import logging
    import pyodbc

    from functools import wraps
    from abc import ABC, abstractmethod
    from enum import Enum
    from logging import StreamHandler

    import uuid
    from datetime import datetime, timezone
    from random import randint
    import datetime

    from faker import Faker
    import random

    print("All ok ")

except Exception as e:
    raise Exception("Error: {} ".format(e))


class Logging:
    """
    This class is used for logging data to datadog an to the console.
    """

    def __init__(self, service_name, ddsource, logger_name="demoapp"):

        self.service_name = service_name
        self.ddsource = ddsource
        self.logger_name = logger_name

        format = "[%(asctime)s] %(name)s %(levelname)s %(message)s"
        self.logger = logging.getLogger(self.logger_name)
        formatter = logging.Formatter(format, )

        if logging.getLogger().hasHandlers():
            logging.getLogger().setLevel(logging.INFO)
        else:
            logging.basicConfig(level=logging.INFO)


global logger
logger = Logging(service_name="database-common-module", ddsource="database-common-module",
                 logger_name="database-common-module")


def error_handling_with_logging(argument=None):
    def real_decorator(function):
        @wraps(function)
        def wrapper(self, *args, **kwargs):
            function_name = function.__name__
            response = None
            try:
                if kwargs == {}:
                    response = function(self)
                else:
                    response = function(self, **kwargs)
            except Exception as e:
                response = {
                    "status": -1,
                    "error": {"message": str(e), "function_name": function.__name__},
                }
                logger.logger.info(response)
            return response

        return wrapper

    return real_decorator


class DatabaseInterface(ABC):
    @abstractmethod
    def get_data(self, query):
        """
        For given query fetch the data
        :param query: Str
        :return: Dict
        """

    def execute_non_query(self, query):
        """
        Inserts data into SQL Server
        :param query:  Str
        :return: Dict
        """

    def insert_many(self, query, data):
        """
        Insert Many items into database
        :param query: str
        :param data: tuple
        :return: Dict
        """

    def get_data_batch(self, batch_size=10, query=""):
        """
        Gets data into batches
        :param batch_size: INT
        :param query: STR
        :return: DICT
        """

    def get_table(self, table_name=""):
        """
        Gets the table from database
        :param table_name: STR
        :return: OBJECT
        """


class Settings(object):
    """settings class"""

    def __init__(
            self,
            port="",
            server="",
            username="",
            password="",
            timeout=100,
            database_name="",
            connection_string="",
            collection_name="",
            **kwargs,
    ):
        self.port = port
        self.server = server
        self.username = username
        self.password = password
        self.timeout = timeout
        self.database_name = database_name
        self.connection_string = connection_string
        self.collection_name = collection_name


class DatabaseSQLServer(DatabaseInterface):
    """SQL database class"""

    def __init__(self, data_base_settings, autocommit=True):
        self.data_base_settings = data_base_settings
        self.autocommit = autocommit
        if self.data_base_settings.database_name != "":
            self.connectionString = (
                    """Driver={ODBC Driver 17 for SQL Server};"""
                    + """SERVER={};UID={};PWD={};Database={};""".format(
                self.data_base_settings.server,
                self.data_base_settings.username,
                self.data_base_settings.password,
                self.data_base_settings.database_name
            )
            )
        else:
            self.connectionString = (
                    """Driver={ODBC Driver 17 for SQL Server};"""
                    + """PORT={};SERVER={};UID={};PWD={};""".format(
                self.data_base_settings.port,
                self.data_base_settings.server,
                self.data_base_settings.username,
                self.data_base_settings.password
            )
            )
        self.client = pyodbc.connect(
            self.connectionString,
            timeout=self.data_base_settings.timeout,
            autocommit=self.autocommit,
        )
        logger.logger.info("SQL Server connection established successfully.")

    @error_handling_with_logging()
    def get_data(self, query):
        self.query = query
        cursor = self.client.cursor()
        cursor.execute(self.query)
        result = cursor.fetchall()
        columns = [column[0] for column in cursor.description]
        data = [dict(zip(columns, item)) for item in result]
        return {"statusCode": 200, "data": data}

    @error_handling_with_logging()
    def execute_non_query(self, query):
        self.query = query
        cursor = self.client.cursor()
        cursor.execute(self.query)
        self.client.commit()
        return {"statusCode": 200, "data": True}

    @error_handling_with_logging()
    def insert_many(self, query, data):
        self.query = query
        cursor = self.client.cursor()
        cursor.executemany(self.query, data)
        self.client.commit()
        return {"statusCode": 200, "data": True}

    @error_handling_with_logging()
    def get_data_batch(self, batch_size=10, query=""):
        self.query = query
        cursor = self.client.cursor()
        cursor.execute(self.query)
        columns = [column[0] for column in cursor.description]
        while True:
            result = cursor.fetchmany(batch_size)
            if not result:
                break
            else:
                items = [dict(zip(columns, data)) for data in result]
                yield items

    @error_handling_with_logging()
    def get_table(self, table_name=""):
        table = table_name
        return {"statusCode": 200, "table": table}


def main():
    ON_SQL_SERVER = DatabaseSQLServer(
        data_base_settings=Settings(
            server="XXXXXXXX",
            username="XXXXX",
            password="XXXXXXXXX",
            database_name="edw"
        )
    )

    states = ("AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN",
              "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
              "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA",
              "WA", "WV", "WI", "WY")
    shipping_types = ("Free", "3-Day", "2-Day")

    product_categories = ("Garden", "Kitchen", "Office", "Household")
    referrals = ("Other", "Friend/Colleague", "Repeat Customer", "Online Ad")

    for i in range(0, 2):
        item_id = random.randint(1, 1000000)
        state = states[random.randint(0, len(states) - 1)]
        shipping_type = shipping_types[random.randint(0, len(shipping_types) - 1)]
        product_category = product_categories[random.randint(0, len(product_categories) - 1)]
        quantity = random.randint(1, 4)
        referral = referrals[random.randint(0, len(referrals) - 1)]
        price = random.randint(1, 100)
        order_date = datetime.date(2016, random.randint(1, 12), random.randint(1, 26)).isoformat()
        invoiceid = random.randint(1, 20000)

        cnxn = ON_SQL_SERVER.client
        cursor = cnxn.cursor()

        query = 'INSERT INTO invoice (invoiceid, itemid, category, price, quantity, orderdate, destinationstate, shippingtype, referral) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)'
        data_order = (invoiceid, item_id, product_category, price, quantity, order_date, state, shipping_type, referral)

        cursor.execute(query, data_order)


main()
