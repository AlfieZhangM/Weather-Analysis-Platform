from kafka import KafkaConsumer
import report_pb2

def main():
    # Create consumer
    consumer = KafkaConsumer(
        'temperatures',                         # topic name
        bootstrap_servers=['localhost:9092'],   # kafka broker address
        group_id='debug',                      # consumer group name
        enable_auto_commit=True,               # auto commit offsets
        auto_offset_reset='latest'             # don't read from beginning
    )

    # Continuously print messages
    for message in consumer:
        try:
            # Parse protobuf message
            report = report_pb2.Report()
            report.ParseFromString(message.value)
            
            # Print in required format
            print({
                'station_id': message.key.decode(),
                'date': report.date,
                'degrees': report.degrees,
                'partition': message.partition
            })
        except Exception as e:
            print(f"Error processing message: {e}")

if __name__ == "__main__":
    main()