"""
Streaming data consumer
"""
from datetime import datetime
from kafka import KafkaConsumer
import mysql.connector

TOPIC = 'toll_data_topic'
DATABASE = 'tolldata'
USERNAME = 'omitted'
PASSWORD = 'omitted'
HOST = 'omitted'

print("Connecting to the database")
try:
    connection = mysql.connector.connect(
        host=HOST,
        database=DATABASE,
        user=USERNAME,
        password=PASSWORD
    )
    cursor = connection.cursor()
    print("Connected to database")
except mysql.connector.Error as err:
    print(f"Could not connect to database. Please check credentials: {err}")
    exit(1)

print("Connecting to Kafka")
consumer = KafkaConsumer(TOPIC, bootstrap_servers='localhost:9092')
print("Connected to Kafka")
print(f"Reading messages from the topic {TOPIC}")

for msg in consumer:
    # Extract information from Kafka
    message = msg.value.decode("utf-8")
    # Transform the date format to suit the database schema
    (timestamp, vehicle_id, vehicle_type, toll_plaza_id) = message.split(",")

    dateobj = datetime.strptime(timestamp, '%a %b %d %H:%M:%S %Y')
    timestamp = dateobj.strftime("%Y-%m-%d %H:%M:%S")

    # Loading data into the database table
    sql = "INSERT INTO livetolldata (timestamp, vehicle_id, vehicle_type, toll_plaza_id) VALUES (%s, %s, %s, %s)"
    cursor.execute(sql, (timestamp, vehicle_id, vehicle_type, toll_plaza_id))
    print(f"A {vehicle_type} was inserted into the database")
    connection.commit()

cursor.close()
connection.close()