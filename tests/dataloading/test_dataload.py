import os
import pandas as pd
import pytest
from unittest.mock import patch, MagicMock
from scripts.dataloading.dataload import process_and_persist_data  # Replace 'your_module' with the actual module where process_and_persist_data is defined

@pytest.fixture
def mock_db_connector():
    return MagicMock()

@patch('your_module.DatabaseConnector', autospec=True)
def test_process_and_persist_data(mock_db_connector, tmpdir):
    main_directory = str(tmpdir)
    database_uri = "postgresql://test_user:test_password@localhost:15432/test_database"  # Update with test PostgreSQL connection details

    # Create test CSV files in the temporary directory
    csv_data = {'Column1': [1, 2, 3], 'Column2': ['a', 'b', 'c']}
    pd.DataFrame(csv_data).to_csv(os.path.join(main_directory, 'Viewer-Data', 'test_chart.csv'), index=False)

    # Mocking the persist_dataframe method of DatabaseConnector to avoid actual database writes in the test
    with patch.object(mock_db_connector.return_value, 'persist_dataframe') as mock_persist_dataframe:
        # Call the function with the mock DatabaseConnector
        process_and_persist_data(main_directory, database_uri)

        # Assertions
        mock_persist_dataframe.assert_any_call(pd.DataFrame(csv_data), 'viewer_age_table')
        mock_persist_dataframe.assert_any_call(pd.DataFrame(csv_data), 'content_detail')
        mock_persist_dataframe.assert_any_call(pd.DataFrame(csv_data), 'demographics')
        mock_persist_dataframe.assert_any_call(pd.DataFrame(csv_data), 'geography')
        mock_persist_dataframe.assert_any_call(pd.DataFrame(csv_data), 'device')
        mock_persist_dataframe.assert_any_call(pd.DataFrame(csv_data), 'subscribe')
        mock_persist_dataframe.assert_any_call(pd.DataFrame(csv_data), 'sharing')

if __name__ == '__main__':
    pytest.main()
