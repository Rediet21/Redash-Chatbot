import pandas as pd
from sqlalchemy import create_engine, MetaData, Table

class DatabaseConnector:
    def __init__(self, database_uri):
        """
        Initializes the DatabaseConnector with the provided database URI.

        Parameters:
        - database_uri (str): The URI to connect to the database.
        """
        self.engine = create_engine(database_uri)
        self.metadata = MetaData(bind=self.engine)

    def persist_dataframe(self, dataframe, table_name, if_exists='replace', index=False):
        """
        Persists a Pandas DataFrame to a specified table in the database.

        Parameters:
        - dataframe (pd.DataFrame): The DataFrame to be persisted.
        - table_name (str): The name of the table in the database.
        - if_exists (str, optional): Action to take if the table already exists. 
          Valid values are 'fail', 'replace', and 'append'. Default is 'replace'.
        - index (bool, optional): Write DataFrame index as a column. Default is False.
        """
        dataframe.to_sql(table_name, self.engine, if_exists=if_exists, index=index)

    def read_table(self, table_name, columns=None, condition=None):
        """
        Reads data from a specified table in the database.

        Parameters:
        - table_name (str): The name of the table in the database.
        - columns (list, optional): A list of columns to select. Default is None (all columns).
        - condition (str, optional): A SQL WHERE clause to filter rows. Default is None.

        Returns:
        - pd.DataFrame: The retrieved data as a Pandas DataFrame.
        """
        query = f"SELECT {', '.join(columns) if columns else '*'} FROM {table_name}"
        if condition:
            query += f" WHERE {condition}"

        return pd.read_sql_query(query, self.engine)

    def update_table(self, table_name, update_dict, condition):
        """
        Updates rows in a specified table based on a given condition.

        Parameters:
        - table_name (str): The name of the table in the database.
        - update_dict (dict): A dictionary of column-value pairs to update.
        - condition (str): A SQL WHERE clause to filter rows to be updated.
        """
        table = Table(table_name, self.metadata, autoload=True)
        update_statement = table.update().where(condition).values(update_dict)
        self.engine.execute(update_statement)

    def delete_rows(self, table_name, condition):
        """
        Deletes rows from a specified table based on a given condition.

        Parameters:
        - table_name (str): The name of the table in the database.
        - condition (str): A SQL WHERE clause to filter rows to be deleted.
        """
        table = Table(table_name, self.metadata, autoload=True)
        delete_statement = table.delete().where(condition)
        self.engine.execute(delete_statement)
