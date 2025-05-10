import psycopg2
from confluent_kafka import SerializingProducer, KafkaError, KafkaException, Consumer
import simplejson as json
from main import delivery_report
import random
from datetime import datetime
if __name__ == "__main__":

    # Create a Kafka consumer and producer
    consumer = Consumer({'bootstrap.servers': 'localhost:9092'} 
                        | {'group.id': 'voting_group', 
                        'auto.offset.reset': 'earliest', 
                        'enable.auto.commit': False
                        })
    # Create a Kafka producer
    producer = SerializingProducer({'bootstrap.servers': 'localhost:9092'})
    

    # Connect to the PostgreSQL database
    database_connection = psycopg2.connect('host=localhost dbname=Election_Database user=postgres password=postgres')
    cursor = database_connection.cursor()

    # Fetch voter details from the database
    candidate_query = cursor.execute("""SELECT row_to_json(columns) FROM (SELECT * FROM CANDIDATES_REGISTRATION) columns; """)
    
    candidate = [candidate[0] for candidate in cursor.fetchall()]
    
    # Check if the candidate details were fetched successfully
    if len(candidate) == 0:
        raise Exception("No candidate details found in the database.")
    else:
        print(candidate)

    Consumer.subscribe(['voter_registration'])

    # Poll for messages from the Kafka topic
    try:
        while True:
            message = consumer.poll(1.0)  # Wait for a message for 1 second
            if message is None:
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    raise KafkaException(message.error())
            else:
                voter = json.loads(message.value().decode('utf-8')) 
                chosen_candidate = random.choice(candidate)
                vote = voter | chosen_candidate | {"Voting_time": datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S'), 'voted': 1}
                print(vote)

    except Exception as e:
        print(f"Error: {e}")
    finally:        
        # Close the consumer and producer
        consumer.close()
        producer.flush()
        database_connection.close()


