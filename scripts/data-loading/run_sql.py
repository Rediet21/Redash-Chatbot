import os
import glob
import sqlalchemy

def execute_sql_scripts(database_uri, sql_directory):
    engine = sqlalchemy.create_engine(database_uri)

    # Get all SQL files in the specified directory
    sql_files = glob.glob(os.path.join(sql_directory, '*.sql'))

    # Execute SQL statements from each file
    for sql_file in sql_files:
        with open(sql_file, 'r') as file:
            sql_statements = file.read()

        # Split statements based on semicolons
        statements = sql_statements.split(';')

        # Execute each statement
        with engine.connect() as connection:
            for statement in statements:
                if statement.strip():
                    connection.execute(statement)

if __name__ == "__main__":
    database_uri = "sqlite:///youtube_data.db"  # SQLite example, replace with your database URI
    sql_directory = "sql_scripts"  # Replace with the actual path to your SQL scripts directory

    execute_sql_scripts(database_uri, sql_directory)
