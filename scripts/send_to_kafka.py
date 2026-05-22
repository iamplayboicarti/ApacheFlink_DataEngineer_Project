from confluent_kafka import Producer
import csv
import time

producer = Producer({'bootstrap.servers': 'localhost:9092'})
with open('Datasets/order_items.csv', 'r') as file:
    reader = csv.reader(file)
    next(reader)  # Skip header
    for row in reader:
        producer.produce('sales-topic', value=','.join(row).encode('utf-8'))
        producer.flush()
        time.sleep(1)  # Simulate streaming by sending every 1s
