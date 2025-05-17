import psycopg2
from confluent_kafka import SerializingProducer, KafkaError, KafkaException, Consumer
import simplejson as json
from main import delivery_report
import random
from datetime import datetime, timezone
import time

# Create a Kafka consumer
consumer = Consumer({'bootstrap.servers': 'localhost:9092'} 
                    | {'group.id': 'voting_group', 
                    'auto.offset.reset': 'earliest', 
                    'enable.auto.commit': False
                    })

# Create a Kafka producer
producer = SerializingProducer({'bootstrap.servers': 'localhost:9092'})

if __name__ == "__main__":
    # Connect to the PostgreSQL database
    database_connection = psycopg2.connect('host=localhost dbname=Election_Database user=postgres password=postgres')
        # Check if the connection was successful
    if database_connection.status != 1:
        print("Failed to connect to the database.")
        exit(1)
    else:
        print("Connected to the database successfully.")
        
    # Create a cursor to execute SQL queries
    cursor = database_connection.cursor()

    # Fetch voter details from the database
    cursor.execute("""SELECT row_to_json(columns) FROM (SELECT * FROM CANDIDATES_REGISTRATION) columns; """)
    
    candidate = [candidate[0] for candidate in cursor.fetchall()]
    
    # Check if the candidate details were fetched successfully
    if len(candidate) == 0:
        raise Exception("No candidate details found in the database.")
    

    consumer.subscribe(['voter_registration'])

    # Poll for messages from the Kafka topic
    try:
        while True:
            # Poll for a message from the Kafka topic
            message = consumer.poll(5.0)  # Wait for a message for 1 second
            # If no message is received, continue polling
            if message is None:
                continue
            # If an error occurs, raise an exception
            elif message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    raise KafkaException(message.error())
            # If a message is received, process it
            # Check if the message is a valid JSON object
            else:
                # Decode the message value and load it as a JSON object
                voter = json.loads(message.value().decode('utf-8')) 
                # Check if the voter has already voted
                chosen_candidate = random.choice(candidate)

                # Check if the voter has already voted
                cursor.execute("SELECT COUNT(*) FROM VOTES_REGISTRATION WHERE voter_id = %s", (voter["voter_id"],))
                vote_count = cursor.fetchone()[0]
                if vote_count > 0:
                    print(f"Voter {voter['voter_id']} has already voted.")
                    continue

                # Check if the candidate is valid
                cursor.execute("SELECT COUNT(*) FROM CANDIDATES_REGISTRATION WHERE candidate_id = %s", (chosen_candidate["candidate_id"],))
                candidate_count = cursor.fetchone()[0]
                if candidate_count == 0:
                    print(f"Candidate {chosen_candidate['candidate_id']} is not valid.")
                    continue

                # Check if the voter is valid
                cursor.execute("SELECT COUNT(*) FROM VOTER_REGISTRATION WHERE voter_id = %s", (voter["voter_id"],))
                voter_count = cursor.fetchone()[0]
                if voter_count == 0:
                    print(f"Voter {voter['voter_id']} is not valid.")
                    continue

                
                vote = voter | chosen_candidate | {"voting_time": datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'), 'vote': 1}
                
                # Insert the vote into the database and produce it to Kafka
                try:
                    print(f"Vote for voter {vote['voter_id']} is being processed...")
                    # Insert the vote into the database
                    cursor.execute(
                        """INSERT INTO VOTES_REGISTRATION (voter_id, candidate_id, voting_time, vote) 
                        VALUES (%s, %s, %s, %s)""",
                        (vote["voter_id"], vote["candidate_id"], vote["voting_time"], vote["vote"])
                    )
                    # Commit the transaction
                    database_connection.commit()
                    print(f"Vote for voter {vote['voter_id']} has been successfully inserted into the database.")
                
                    # Produce the vote to Kafka before committing to the database
                    producer.produce(
                        topic='votes_topic',
                        key=str(vote["voter_id"]),
                        value=json.dumps(vote), # Convert the vote to JSON format
                        on_delivery=delivery_report
                    )
                    producer.poll(0)  # Poll the producer to handle delivery reports
                    # Flush the producer to ensure all messages are sent
                    producer.flush()  # Ensure the message is sent to Kafka
                    print(f"Vote for voter {vote['voter_id']} has been successfully inserted into the database and produced to Kafka.")

                except Exception as e:
                    print(f"Error inserting vote for voter {vote['voter_id']}: {e}")

            time.sleep(3) # Sleep for 3 seconds before processing the next vote
    except Exception as e:
        print(f"Error: {e}")


