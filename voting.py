import psycopg2
import os
from dotenv import load_dotenv
import requests
import random
import time
import simplejson as json
from datetime import datetime
from confluent_kafka import SerializingProducer, Consumer, KafkaException, KafkaError
from main import delivery_report

load_dotenv()

PG_HOST = os.getenv('PG_HOST')
PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD')
PG_DATABASE = os.getenv('PG_DATABASE')

conf = {
    'bootstrap.servers': 'localhost:9092'
}

consumer = Consumer(conf | {
    'group.id': 'voting-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
})

producer = SerializingProducer(conf)

if __name__ == "__main__":
    conn = psycopg2.connect(f" \
        host={PG_HOST} \
        dbname={PG_DATABASE} \
        user={PG_USER} \
        password={PG_PASSWORD} \
    ")
    
    cur = conn.cursor()
    
    candidates_query = cur.execute(
        """
        SELECT row_to_json(col)
        FROM (
            SELECT * FROM candidates
        ) col;
        """
    )
    
    candidates = [candidate[0] for candidate in cur.fetchall()]

    if len(candidates) == 0:
        raise Exception("No candidates found in the database")
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
                    'voting_time': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
                    'vote': 1
                }
                
                try:
                    # Need to insert into database first if there is any conflict 
                    # -> the data will not be inserted into database and NOT produce to Kafka topic
                    # Otherwise, if produce to Kafka first and insert into database later
                    # If there is any conflict in DB -> inconsistent data in DB compared to Kafka topic
                    
                    print(f"User {vote['voter_id']} is voting for candidate: {vote['candidate_id']}")
                    cur.execute("""
                        INSERT INTO votes (voter_id, candidate_id, voting_time)
                        VALUES(%s, %s, %s)
                    """, (vote['voter_id'], vote['candidate_id'], vote['voting_time']))
                    conn.commit()
                    
                    producer.produce(
                        'votes_topic',
                        key=vote['voter_id'],
                        value=json.dumps(vote),
                        on_delivery=delivery_report
                    )
                    
                    producer.poll(0)
                    
                except Exception as e:
                    print('Error', e)
            time.sleep(0.5)
    except Exception as e:
        print(e)
        
    