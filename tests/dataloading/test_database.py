import unittest
import pandas as pd
from sqlalchemy import create_engine
from unittest.mock import patch
import unittest
import pandas as pd
from sqlalchemy import create_engine
from unittest.mock import patch
from scripts.dataloading.database import DatabaseConnector  # Make sure to replace 'your_module' with the actual module where DatabaseConnector is defined

class TestDatabaseConnector(unittest.TestCase):

    def setUp(self):
        # Replace 'your_database_uri' with the actual database URI for testing
        self.test_database_uri = 'your_database_uri'
        self.connector = DatabaseConnector(self.test_database_uri)

    def tearDown(self):
        # Clean up any resources if needed
        pass

    def test_persist_dataframe(self):
        # Mocking the to_sql method of Pandas DataFrame to avoid actual database writes in the test
        with patch.object(pd.DataFrame, 'to_sql') as mock_to_sql:
            # Test DataFrame
            test_data = {'column1': [1, 2, 3], 'column2': ['a', 'b', 'c']}
            test_dataframe = pd.DataFrame(test_data)

            # Test persist_dataframe method
            self.connector.persist_dataframe(test_dataframe, 'test_table')

            # Assertions
            mock_to_sql.assert_called_once_with('test_table', self.connector.engine, if_exists='replace', index=False)

    def test_read_table(self):
        # Mocking the read_sql_query method of Pandas to avoid actual database reads in the test
        with patch('pandas.read_sql_query') as mock_read_sql_query:
            # Test read_table method
            self.connector.read_table('test_table', columns=['column1', 'column2'], condition='column1 > 1')

            # Assertions
            mock_read_sql_query.assert_called_once_with('SELECT column1, column2 FROM test_table WHERE column1 > 1', self.connector.engine)

    # Add similar test methods for other DatabaseConnector methods

if __name__ == '__main__':
    unittest.main()
  # Make sure to replace 'your_module' with the actual module where DatabaseConnector is defined

class TestDatabaseConnector(unittest.TestCase):

    def setUp(self):
        # Replace 'your_database_uri' with the actual database URI for testing
        self.test_database_uri = 'your_database_uri'
        self.connector = DatabaseConnector(self.test_database_uri)

    def tearDown(self):
        # Clean up any resources if needed
        pass

    def test_persist_dataframe(self):
        # Mocking the to_sql method of Pandas DataFrame to avoid actual database writes in the test
        with patch.object(pd.DataFrame, 'to_sql') as mock_to_sql:
            # Test DataFrame
            test_data = {'column1': [1, 2, 3], 'column2': ['a', 'b', 'c']}
            test_dataframe = pd.DataFrame(test_data)

            # Test persist_dataframe method
            self.connector.persist_dataframe(test_dataframe, 'test_table')

            # Assertions
            mock_to_sql.assert_called_once_with('test_table', self.connector.engine, if_exists='replace', index=False)

    def test_read_table(self):
        # Mocking the read_sql_query method of Pandas to avoid actual database reads in the test
        with patch('pandas.read_sql_query') as mock_read_sql_query:
            # Test read_table method
            self.connector.read_table('test_table', columns=['column1', 'column2'], condition='column1 > 1')

            # Assertions
            mock_read_sql_query.assert_called_once_with('SELECT column1, column2 FROM test_table WHERE column1 > 1', self.connector.engine)

    # Add similar test methods for other DatabaseConnector methods

if __name__ == '__main__':
    unittest.main()
