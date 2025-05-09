import psycopg2
import requests
# import random

# random.seed(400)
BASE_URL = 'https://randomuser.me/api/?nat=ca'

parties = [
    "Liberal Party of Canada",
    "Conservative Party of Canada",
    "New Democratic Party (NDP)",
    "Bloc Québécois",
    "Green Party of Canada",
    "People’s Party of Canada (PPC)"
]

bios = [
    "As leader of the Liberal Party, I am committed to building a strong middle class, advancing reconciliation, and tackling climate change with bold and balanced policies. Canada’s future must be inclusive, innovative, and sustainable.",

    "I’m running to bring common-sense leadership back to Ottawa. I will make life more affordable, restore fiscal responsibility, and ensure public safety while protecting the values that make Canada strong.",

    "I stand with everyday Canadians — workers, students, and families. As leader of the NDP, I’ll fight to make housing, healthcare, and education affordable while holding corporations accountable and building a more just society.",

    "As leader of the Bloc Québécois, I am here to defend the interests and identity of Quebec. Our priority is ensuring Quebec’s voice is respected, our culture protected, and our autonomy strengthened within Canada.",

    "Canada needs a future-focused government rooted in environmental responsibility and social justice. As Green leader, I will prioritize climate action, Indigenous rights, and a sustainable economy for generations to come.",

    "I represent a return to freedom, personal responsibility, and real Canadian values. The People’s Party is against government overreach and is focused on ending inflationary spending, protecting free speech, and putting Canada first."
]

platforms = [
    "1. Affordable Child Care – Expand access to $10-a-day child care nationwide.\n"
    "2. Climate Leadership – Reach net-zero emissions by 2050 and invest in clean energy.\n"
    "3. Inclusive Growth – Support the middle class with housing, education, and tax relief.",

    "1. Axe the Carbon Tax – Eliminate the federal carbon tax to ease the cost of living.\n"
    "2. Restore Fiscal Balance – Cut government waste and reduce the deficit.\n"
    "3. Safer Streets – Enforce stronger penalties for repeat violent offenders.",

    "1. Universal Pharmacare – Launch a national drug coverage program for all Canadians.\n"
    "2. Affordable Housing – Build 500,000 affordable homes and stop corporate landlords.\n"
    "3. Tax the Rich – Make the ultra-wealthy and big corporations pay their fair share.",

    "1. Defend Quebec’s Autonomy – Increase provincial powers over language, immigration, and culture.\n"
    "2. Protect French – Strengthen French language laws and support francophone communities.\n"
    "3. Promote Green Industry in Quebec – Invest in Quebec-based sustainable projects.",

    "1. Bold Climate Action – End subsidies to oil companies and transition to renewables.\n"
    "2. Fair Economy – Introduce a guaranteed livable income and wealth taxes.\n"
    "3. Reconciliation – Fully implement the TRC Calls to Action and MMIWG Inquiry recommendations.",

    "1. End Mandates and Censorship – Protect medical freedom and freedom of speech.\n"
    "2. Eliminate the Carbon Tax – Lower fuel costs and defend energy workers.\n"
    "3. Shrink Government – Cut foreign aid, CBC funding, and federal bureaucracy."
]



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
                    registered_age INTEGER)
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
        CREATE TABLE IF NOT EXISTS VOTES_REGISTRATION (
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
        "registered_age": voter["registered"]["age"]
    }



def inser_voter_details(database_connection, cursor, voter_data):
    # Insert voter details into the database
    cursor.execute("""
        INSERT INTO VOTER_REGISTRATION(voter_id, voter_name, date_of_birth,
                   gender,nationality,registration_number,address_street,address_city,
                   address_state,address_country,address_postcode,email,phone_number,cell_number,picture,registered_age)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (voter_data["voter_id"], voter_data["voter_name"], voter_data["date_of_birth"], voter_data["gender"], 
          voter_data["nationality"], voter_data["registration_number"], voter_data["address"]["street"], 
          voter_data["address"]["city"], voter_data["address"]["state"], voter_data["address"]["country"], 
          voter_data["address"]["postcode"], voter_data["email"], voter_data["phone_number"], 
          voter_data["cell_number"], voter_data["picture"], voter_data["registered_age"]))
    database_connection.commit()
               

def inser_candidate_details(database_connection, cursor, candidate_data):
    # Insert candidate details into the database
    cursor.execute("""
        INSERT INTO CANDIDATES_REGISTRATION(candidate_id, candidate_name, party_affiliation, biography, campaign_platform, photo_url)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (candidate_data["candidate_id"], candidate_data["candidate_name"], candidate_data["party_affiliation"], candidate_data["biography"], candidate_data["campaign_platform"], candidate_data["photo_url"]))
    database_connection.commit()


if __name__ == "__main__":
    try:
        # Connect to the PostgreSQL database
        # Replace with your actual database connection details
        database_connection = psycopg2.connect('host=localhost dbname=Election_Database user=postgres password=postgres')
        cursor = database_connection.cursor()
        # Create the table if it doesn't exist
        create_table(database_connection, cursor)
        cursor.execute("SELECT COUNT(*) FROM CANDIDATES_REGISTRATION")
        candidate = cursor.fetchall()
        
        # Check if the table is empty
        # If the table is empty, generate candidate details and insert them into the database
        if candidate[0][0] == 0:
            # Insert candidate details into the database
            for i in range(6):
                candidate_data = generate_candidate_deatails(i)
                inser_candidate_details(database_connection, cursor, candidate_data)
                print(f"Candidate {i+1} details inserted successfully.")
        else:
            print("Candidate details already exist in the database.")
        

        for x in range(1000):
            voter_data = generate_voter_details()
            # Insert voter details into the database
            inser_voter_details(database_connection, cursor, voter_data)
            print(f"Voter {x+1} details inserted successfully.")
        print("Voter and candidate details have been successfully inserted into the database.")


        
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        exit(1)