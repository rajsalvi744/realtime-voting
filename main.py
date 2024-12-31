import psycopg2
import simplejson as json
import requests
from confluent_kafka import SerializingProducer

BASE_URL = "https://randomuser.me/api/?nat=gb"
NO_OF_VOTERS = 50
PARTIES = ['Republican Party', "Democratic Party", "Libertarians"]


def create_tables(conn, cur):
    cur.execute("""
                CREATE TABLE IF NOT EXISTS candidates(
                    candidate_id VARCHAR(255) PRIMARY KEY,
                    candidate_name VARCHAR(255),
                    party_affiliation VARCHAR(255),
                    photo_url TEXT
                )            
                """)
    cur.execute("""
                CREATE TABLE IF NOT EXISTS voters(
                    voter_id VARCHAR(255) PRIMARY KEY,
                    voter_name VARCHAR(255),
                    date_of_birth VARCHAR(255),
                    gender VARCHAR(255),
                    nationality VARCHAR(255),
                    address_street VARCHAR(255),
                    address_city VARCHAR(255),
                    address_state VARCHAR(255),
                    address_country VARCHAR(255),
                    address_postcode VARCHAR(255),
                    phone_number VARCHAR(255),
                    email VARCHAR(255),
                    picture TEXT
                )
                """)
    cur.execute("""
                CREATE TABLE IF NOT EXISTS votes(
                    voter_id VARCHAR(255) UNIQUE,
                    candidate_id VARCHAR(255),
                    voting_time TIMESTAMP,
                    vote int DEFAULT 1,
                    PRIMARY KEY(voter_id,candidate_id)
                )
                """)
    conn.commit()


def generate_candidate_data(candidate_number, no_of_parties):
    res = requests.get(BASE_URL + "&gender=" + ("female" if candidate_number % 2 == 1 else 'male'))
    if res.status_code == 200:
        user_data = res.json()['results'][0]

        return {
            'candidate_id': user_data['login']['uuid'],
            'candidate_name': f"{user_data['name']['first']}{user_data['name']['last']}",
            'party_affiliation': PARTIES[candidate_number],
            'photo_url': user_data['picture']['large'],
        }
    else:
        print("Error Fetching Data")


def generate_voter_data():
    res = requests.get(BASE_URL)
    if res.status_code == 200:
        user_data = res.json()['results'][0]

        return {
            'voter_id': user_data['login']['uuid'],
            'voter_name': f"{user_data['name']['first']}{user_data['name']['last']}",
            'gender': user_data['gender'],
            'nationality': user_data['nat'],
            'date_of_birth': user_data['dob']['date'],
            'address': {
                'street': f"{user_data['location']['street']['number']}{user_data['location']['street']['name']}",
                'city': user_data['location']['city'],
                'state': user_data['location']['state'],
                'country': user_data['location']['country'],
                'postcode': user_data['location']['postcode']
            },
            'email': user_data['email'],
            'phone_number': user_data['phone'],
            'picture': user_data['picture']['large']
        }
    else:
        print("Error Fetching Data")


def insert_candidate_data(candidate, conn, cur):
    cur.execute("""
                INSERT INTO candidates(
                    candidate_id,candidate_name,party_affiliation,photo_url
                )
                VALUES(%s,%s,%s,%s)
                """, (candidate['candidate_id'], candidate['candidate_name'], candidate['party_affiliation'],
                      candidate['photo_url']))

    conn.commit()


def insert_voter_data(voter_data, conn, cur):
    cur.execute("""
                INSERT INTO voters(
                    voter_id,voter_name,gender,nationality,date_of_birth,address_street,address_city,
                    address_state,address_country,address_postcode,email,phone_number,picture
                )
                VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, (voter_data['voter_id'], voter_data['voter_name'], voter_data['gender'], voter_data['nationality'],
                      voter_data['date_of_birth'], voter_data['address']['street'], voter_data['address']['city'],
                      voter_data['address']['state'], voter_data['address']['country'],
                      voter_data['address']['postcode'],
                      voter_data['email'], voter_data['phone_number'], voter_data['picture']))
    conn.commit()


def delivery_report(err, msg):
    if err is not None:
        print(f"Message Delivery Failed {err}")
    else:
        print(f"Message Delivered to {msg.topic}[{msg.partition()}]")


if __name__ == "__main__":
    conn = psycopg2.connect("host = localhost dbname = voting user = postgres password = postgres")
    cur = conn.cursor()
    producer = SerializingProducer({"bootstrap.servers": 'localhost:9092'})

    create_tables(conn, cur)

    cur.execute("""
                SELECT * FROM candidates
                """)
    candidates = cur.fetchall()
    print(candidates)

    if len(candidates) == 0:
        for i in range(len(PARTIES)):
            candidate = generate_candidate_data(i, len(PARTIES))
            print(candidate)
            insert_candidate_data(candidate, conn, cur)

    for i in range(NO_OF_VOTERS):
        voter_data = generate_voter_data()
        insert_voter_data(voter_data, conn, cur)

        producer.produce(
            'voters_topic',
            key=voter_data['voter_id'],
            value=json.dumps(voter_data),
            on_delivery=delivery_report
        )

        print("Produced Voter {}, data : {}".format(i + 1, voter_data))
        producer.flush()

