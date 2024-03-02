from flask import request, jsonify
from redash.handlers.base import (
    BaseResource
)
import os
#from openai import OpenAI
#usage of langchain
from langchain.llms.openai import OpenAI

from langchain.agents import create_sql_agent
from langchain.agents.agent_toolkits import SQLDatabaseToolkit
from langchain.agents import AgentExecutor

from langchain.sql_database import SQLDatabase
import psycopg2

VARIABLE_KEY = os.environ.get("OPENAI_API_KEY")
client = OpenAI(
  api_key=VARIABLE_KEY
)

# db = SQLDatabase.from_uri('postgresql+psycopg2://admin:password123@localhost/admin')
db = SQLDatabase.from_uri('postgresql://postgres@localhost:15432/new')
#from langchain.chat_models import ChatOpenAI
llm = ChatOpenAI(model_name="gpt-3.5-turbo")

     

toolkit = SQLDatabaseToolkit(db=db,llm=llm)

# Create LangChain SQL agent executor
agent_executor = create_sql_agent(
    llm=llm,
    toolkit=toolkit,
    verbose=True
)

#agent_executor.run(" ")
class ChatResource(BaseResource):
    def post(self):
        try:
            value = request.get_json()
            question = value.get('question')
            # Execute the LangChain agent to interact with the database
            langchain_response = agent_executor.run(question)

            completion = client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "system", "content": "You are a redash visualization assistant, skilled in SQL queries and data visualization. You are only required to give answers for query and data visualization questions. If asked about a topic outside these two, make sure to respond that you have no information regarding that question. I am only here to help you with your query and data visualization questions. When asked to write queries, only provide the code without descriptions."},
                    {"role": "user", "content": question}
                ]
            )
           
            #answer = completion.choices[0].message.content
            response_data = {"answer": langchain_response}
            
            return jsonify(response_data), 200
        except Exception as error:
            print(error)
            return jsonify({"error": "An error occurred"}), 500