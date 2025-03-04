import time
from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import UnknownTopicOrPartitionError
import weather
import report_pb2

broker = 'localhost:9092'
admin_client = KafkaAdminClient(bootstrap_servers=[broker])

try:
    admin_client.delete_topics(["temperatures"])
    print("Deleted topics successfully")
except UnknownTopicOrPartitionError:
    print("Cannot delete topic/s (may not exist yet)")

time.sleep(5) # Deletion sometimes takes a while to reflect

# TODO: Create topic 'temperatures' with 4 partitions and replication factor = 1

topic = NewTopic(
    name="temperatures",
    num_partitions=4,
    replication_factor=1
)

admin_client.create_topics([topic])

print("Topics:", admin_client.list_topics())

producer=KafkaProducer(
    bootstrap_servers=[broker],
    retries=10,
    acks='all',
)

for date, degrees, station_id in weather.get_next_weather(delay_sec=0.1):
    report = report_pb2.Report()
    report.date=date
    report.degrees=degrees
    report.station_id=station_id

    value=report.SerializeToString()

    producer.send(
        topic="temperatures",
        key=station_id.encode(),
        value=value
    )

    producer.flush() 
