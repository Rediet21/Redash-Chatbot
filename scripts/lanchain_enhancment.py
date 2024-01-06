import json
import sqlite3
from langchain import LangChain
from sql_generate import ask_database

# LangChain enhancement
def execute_function_chain(message, conn):
    chain = LangChain()

    # Define language chain steps
    chain.add_step("Check if tool call exists", lambda: "ask_database" in [call["function"]["name"] for call in message["tool_calls"]])
    chain.add_step("Extract query from tool call", lambda: json.loads(message["tool_calls"][0]["function"]["arguments"])["query"])
    chain.add_step("Execute ask_database function", lambda query: ask_database(conn, query))
    chain.add_step("Handle non-existent function", lambda: f"Error: function {message['tool_calls'][0]['function']['name']} does not exist")

    # Execute the language chain
    results = chain.execute()

    return results

# Usage example
conn = sqlite3.connect("your_database.db")

message = {
    "tool_calls": [
        {
            "function": {
                "name": "ask_database",
                "arguments": '{"query": "SELECT * FROM your_table"}'
            }
        }
    ]
}

results = execute_function_chain(message, conn)
print(results)