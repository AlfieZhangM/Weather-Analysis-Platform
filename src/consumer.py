import json
import sys
import os
from kafka import KafkaConsumer, TopicPartition
import report_pb2

def initialize_station_stats():
    """Returns an empty stats dictionary for a station"""
    return {
        "count": 0,
        "sum": 0,
        "avg": 0,
        "start": None,
        "end": None
    }

def load_existing_data(json_path):
    if os.path.exists(json_path):
        with open(json_path,'r') as f:
            return json.load(f)
    return {"offset": 0}

def main():
    if len(sys.argv) < 2:
        print("Usage: python consumer.py <partition_numbers...>")
        return
        
    # Get partition numbers from command line arguments
    partitions = [int(p) for p in sys.argv[1:]]
    
    consumer = KafkaConsumer(
        bootstrap_servers=['localhost:9092'],
        group_id=None,  #WE Do not need this
        enable_auto_commit=False,
        auto_offset_reset='earliest'
    )
    
    # Manually assign partitions
    topic = "temperatures"
    topic_partitions = [TopicPartition(topic, p) for p in partitions]
    consumer.assign(topic_partitions)
    
    # Dictionary to store stats for each partition
    partition_stats = {p: {} for p in partitions}
    for partition in partitions:
        json_path=f"partition-{partition}.json"
        existing_data=load_existing_data(json_path)

        if existing_data:
            #Load what we have already
            partition_stats[partition]=existing_data
            tp=TopicPartition(topic,partition)
            if "offset" in existing_data:
                consumer.seek(tp,existing_data["offset"])
        else:
            #Build new partition and read from the beginning
            partition_stats[partition]={}
            tp=TopicPartition(topic,partition)
            consumer.seek(tp,0)
    
    print(f"Started consuming from partitions: {partitions}")
    
    # Consume messages
    for message in consumer:
        try:
            # Parsing
            report = report_pb2.Report()
            report.ParseFromString(message.value)
            station_id = message.key.decode()
            partition = message.partition
            
            # Initialize stats for new station
            if station_id not in partition_stats[partition]:
                partition_stats[partition][station_id] = initialize_station_stats()
            
            # Updating these 3 data
            stats = partition_stats[partition][station_id]
            stats["count"]+=1
            stats["sum"]+=report.degrees
            stats["avg"]=stats["sum"] / stats["count"]
            
            # Update dates
            if stats["start"] is None or report.date<stats["start"]:
                stats["start"] = report.date
            if stats["end"] is None or report.date>stats["end"]:
                stats["end"] = report.date

            tp=TopicPartition(topic,partition)
            cur_offset=consumer.position(tp)
            partition_stats[partition]["offset"]=cur_offset
                
            # Write updated stats to JSON file
            json_path = f"/src/partition-{partition}.json"
            with open(json_path + ".tmp", "w") as f:
                json.dump(partition_stats[partition], f, indent=2)
            os.rename(json_path + ".tmp", json_path)
            
        except Exception as e:
            print(f"Error processing message: {e}")

if __name__ == "__main__":
    main()