import psycopg2
import os

# Connection parameters
db_params = {
    'host': 'localhost',
    'port': 15432,
    'database': 'new',
    'user': 'postgres',
    'password': '',
}

directory_path = '../sql'

def execute_sql_files(directory_path, connection_params):
    try:
        # Establish a connection
        connection = psycopg2.connect(**connection_params)
        cursor = connection.cursor()
        print("Connected to the database!")

        try:
            # Iterate over all files in the specified directory
            for filename in os.listdir(directory_path):
                if filename.endswith(".sql"):
                    file_path = os.path.join(directory_path, filename)

                    # Read the contents of the SQL file
                    with open(file_path, 'r') as sql_file:
                        sql_query = sql_file.read()

                    # Execute the SQL query
                    cursor.execute(sql_query)
                    print(f"Executed SQL file: {filename}")

            # Commit the changes to the database
            connection.commit()
            print("All SQL files executed successfully.")

        except Exception as e:
            # Rollback in case of an error
            connection.rollback()
            print(f"Error executing SQL files: {e}")

        finally:
            # Close the cursor and connection
            cursor.close()
            connection.close()
            print('Finished!')

    except Exception as e:
        print(f"Error: Unable to connect to the database. {e}")

    finally:
        # Close the connection
        if connection:
            connection.close()
            print("Connection closed.")

if __name__ == "__main__":
    execute_sql_files(directory_path, db_params)
