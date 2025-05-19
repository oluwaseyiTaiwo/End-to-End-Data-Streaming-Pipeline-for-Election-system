import psycopg2
import requests
import random
from confluent_kafka import SerializingProducer
from info import parties, bios, platforms
import json
random.seed(42)  # Set a seed for reproducibility
# Set the base URL for the randomuser.me API
BASE_URL = 'https://randomuser.me/api/?nat=ca'

def create_table(database_connection, cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS VOTER_REGISTRATION (
                    voter_id VARCHAR(255) PRIMARY KEY,
                    voter_name VARCHAR(255),
                    date_of_birth VARCHAR(255),
                    gender VARCHAR(255),
                    nationality VARCHAR(255),
                    registration_number VARCHAR(255),
                    address_street VARCHAR(255),
                    address_city VARCHAR(255),
                    address_state VARCHAR(255),
                    address_country VARCHAR(255),
                    address_postcode VARCHAR(255),
                    email VARCHAR(255),
                    phone_number VARCHAR(255),
                    cell_number VARCHAR(255),
                    picture TEXT,
                    age INTEGER)
                   """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS CANDIDATES_REGISTRATION (
            candidate_id VARCHAR(255) PRIMARY KEY,
            candidate_name VARCHAR(255),
            party_affiliation VARCHAR(255),
            biography TEXT,
            campaign_platform TEXT,
            photo_url TEXT)
                   """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS VOTES_RECORD (
            voter_id VARCHAR(255) UNIQUE,
            candidate_id VARCHAR(255),
            voting_time TIMESTAMP,
            vote int DEFAULT 1,
            PRIMARY KEY (voter_id, candidate_id)
        )
    """)

    database_connection.commit()

def generate_candidate_deatails(i):
    # Generate voter details using the randomuser.me API
    while True:
        # Generate a random user
        response = requests.get(f"{BASE_URL}&min_age=18")
        if response.status_code == 200:
            data = response.json()
            # Check if the API returned results
            if not data["results"]:
                continue
            candidate = data["results"][0]
            break
        else:
            # Handle the case where the API request fails
            print(f"Failed to fetch voter details. Status code: {response.status_code}")
            continue

    return {
        'candidate_id': candidate["login"]["uuid"],
        "candidate_name": f"{candidate['name']['first']} {candidate['name']['last']}",
        "party_affiliation": parties[i],
        "biography": bios[i],
        "campaign_platform": platforms[i],
        "photo_url": candidate["picture"]["large"],
    }

def generate_voter_details():
    # Generate voter details using the randomuser.me API
    while True:
        # Generate a random user
        response = requests.get(f"{BASE_URL}&min_age=18")
        if response.status_code == 200:
            data = response.json()
            # Check if the API returned results
            if not data["results"]:
                continue
            voter = data["results"][0]
            break
        else:
            # Handle the case where the API request fails
            print(f"Failed to fetch voter details. Status code: {response.status_code}")
            continue
           
    return {
        'voter_id': voter["login"]["uuid"],
        "voter_name": f"{voter['name']['first']} {voter['name']['last']}",
        "date_of_birth": voter["dob"]["date"],
        "gender": voter["gender"],
        "nationality": voter["nat"],
        "registration_number": voter["login"]["username"],
        "address": {
            "street": f"{voter['location']['street']['number']} {voter['location']['street']['name']}",
            "city": voter["location"]["city"],
            "state": voter["location"]["state"],
            "country": voter["location"]["country"],
            "postcode": voter["location"]["postcode"]
        },
        "email": voter["email"],
        "phone_number": voter["phone"],
        "cell_number": voter["cell"],
        "picture": voter["picture"]["large"],
        "age": voter["dob"]["age"]
    }



def inser_voter_details(database_connection, cursor, voter_data):
    # Insert voter details into the database
    cursor.execute("""
        INSERT INTO VOTER_REGISTRATION(voter_id, voter_name, date_of_birth,
                   gender,nationality,registration_number,address_street,address_city,
                   address_state,address_country,address_postcode,email,phone_number,cell_number,picture,age)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (voter_data["voter_id"], voter_data["voter_name"], voter_data["date_of_birth"], voter_data["gender"], 
          voter_data["nationality"], voter_data["registration_number"], voter_data["address"]["street"], 
          voter_data["address"]["city"], voter_data["address"]["state"], voter_data["address"]["country"], 
          voter_data["address"]["postcode"], voter_data["email"], voter_data["phone_number"], 
          voter_data["cell_number"], voter_data["picture"], voter_data["age"]))
    database_connection.commit()
               

def inser_candidate_details(database_connection, cursor, candidate_data):
    # Insert candidate details into the database
    cursor.execute("""
        INSERT INTO CANDIDATES_REGISTRATION(candidate_id, candidate_name, party_affiliation, biography, campaign_platform, photo_url)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (candidate_data["candidate_id"], candidate_data["candidate_name"], candidate_data["party_affiliation"], candidate_data["biography"], candidate_data["campaign_platform"], candidate_data["photo_url"]))
    database_connection.commit()

def delivery_report(err, msg):
    # Callback function to handle delivery report
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def voter_id_exists(voter_id, cursor):
    cursor.execute("SELECT 1 FROM VOTER_REGISTRATION WHERE voter_id = %s", (voter_id,))
    return cursor.fetchone() is not None

def generate_unique_voter(cursor):
    while True:
        voter_data = generate_voter_details()
        if not voter_id_exists(voter_data["voter_id"], cursor):
            return voter_data


if __name__ == "__main__":
    # Initialize the Kafka producer
    producer = SerializingProducer({'bootstrap.servers': 'localhost:9092'})
    
    try:
        # Connect to the PostgreSQL database
        # Replace with your actual database connection details
        database_connection = psycopg2.connect('host=localhost dbname=Election_Database user=postgres password=postgres')
        cursor = database_connection.cursor()

        # Check if the connection was successful
        if database_connection.status != 1:
            print("Failed to connect to the database.")
            exit(1)
        else:
            print("Connected to the database successfully.")
        
        
        # Create the tables if they don't exist       
        create_table(database_connection, cursor)

        # Check if the CANDIDATES_REGISTRATION table is empty
        cursor.execute("SELECT COUNT(*) FROM CANDIDATES_REGISTRATION")
        candidate = cursor.fetchall()
        # If the table is empty, generate candidate details and insert them into the database
        if candidate[0][0] == 0:
            # Insert candidate details into the database
            for i in range(6):
                candidate_data = generate_candidate_deatails(i)
                inser_candidate_details(database_connection, cursor, candidate_data)
                print(f"Candidate {i+1} details inserted successfully.")
        else:
            print("Candidate details already exist in the database.")
        

        for x in range(13):
            voter_data = generate_unique_voter(cursor)
            # Insert voter details into the database
            inser_voter_details(database_connection, cursor, voter_data)
            print(f"Voter {x+1} details inserted successfully.")

            # Produce the voter data to Kafka
            # The Kafka topic is 'voter_registration'
            # The key is the voter ID, and the value is the voter data in JSON format
            # This allows other services to consume the voter data for further processing
            producer.produce(
                topic='voter_registration',
                key=voter_data["voter_id"],
                value=json.dumps(voter_data),
                on_delivery=delivery_report
            )
            # Flush the producer to ensure all messages are sent
            producer.flush()
        print("Voter and candidate details have been successfully inserted into the database.")


        
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        exit(1)