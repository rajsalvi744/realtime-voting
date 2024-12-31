from confluent_kafka import SerializingProducer, KafkaError, KafkaException, Consumer
import simplejson as json
from datetime import datetime
import random
import time
import psycopg2
from main import delivery_report

conf = {'bootstrap.servers': 'localhost:9092'}

producer = SerializingProducer(conf)
consumer = Consumer(conf | {
    'group.id': 'voting-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
})

if __name__ == "__main__":
    conn = psycopg2.connect("host = localhost dbname = voting user=postgres password=postgres")
    cur = conn.cursor()

    query = cur.execute("""
                        SELECT row_to_json(col)
                        FROM (
                        SELECT * from candidates
                        ) col;
                        """)

    candidates = [candidate[0] for candidate in cur.fetchall()]

    if len(candidates) == 0:
        raise Exception("No Candidates Found in the Database")
    else:
        print(candidates)

    consumer.subscribe(['voters_topic'])
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            elif msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break
            else:
                voter = json.loads(msg.value().decode('utf-8'))
                chosen_candidate = random.choice(candidates)
                vote = voter | chosen_candidate | {
                    'voting_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    'vote': 1
                }

                try:
                    print("Voter {} is voting for candidate {}".format(vote['voter_id'], vote['candidate_id']))
                    cur.execute("""
                                INSERT INTO votes(voter_id, candidate_id, voting_time)
                                VALUES (%s,%s,%s)
                                """, (vote['voter_id'], vote['candidate_id'], vote['voting_time'])
                                )
                    conn.commit()

                    producer.produce('votes_topic',
                                     key=vote['voter_id'],
                                     value=json.dumps(vote),
                                     on_delivery=delivery_report)
                    producer.flush()
                except Exception as e:
                    print("ERROR", e)
                    continue
            time.sleep(0.2)
    except KafkaException as e:
        print(e)
    finally:
        consumer.close()
