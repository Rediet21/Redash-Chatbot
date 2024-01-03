import pandas as pd
import sqlalchemy

class DatabaseConnector:
    def __init__(self, database_uri):
        self.engine = sqlalchemy.create_engine(database_uri)

    def persist_dataframe(self, dataframe, table_name):
        dataframe.to_sql(table_name, self.engine, if_exists='replace', index=False)
