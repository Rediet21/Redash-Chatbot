from celery import Celery

app = Celery('redash_chatbot', include=['your_module'])

@app.task
def process_message(message):
    
    return f"Processed message: {message}"
