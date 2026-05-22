# Apache Flink for Sales Analytics (Streaming)

This repository contains an **end-to-end data engineering project** using **Apache Flink**, focused on performing **sales analytics in a streaming pipeline**.
The project demonstrates how to ingest sales data from **Kafka**, join it with product data from **CSV**, aggregate sales by category, and sink the results to **Elasticsearch** for visualization in **Kibana**.

---

## 🚀 Project Overview

The project reads:

* **Streaming sales data (order\_items)** from a **Kafka topic**
* Joins it with **static product data** (`products.csv`)
* Computes **total sales per category**
* Pushes **aggregated results** to **Elasticsearch** (`category-sales` index)
* Pushes **raw joined data** to **Elasticsearch** (`order-items-raw` index)

A Kibana dashboard visualizes both **aggregated sales by category** and **raw sales by product**.

---

## ✨ Features

* Streaming data ingestion from **Kafka** (`order_items`)
* Static data ingestion from **CSV** (`products.csv`)
* Use of POJOs (`OrderItem`, `Product`, `CategorySalesDTO`) for data representation
* **Broadcast join** for combining streaming and static data
* **Aggregation by category** (sink → `category-sales`)
* **Raw order items with product details** (sink → `order-items-raw`)
* Output to **Elasticsearch** for real-time visualization in **Kibana**
* **Dockerized environment** with Kafka, Zookeeper, Elasticsearch, and Kibana

---

## ⚙️ Getting Started

### Prerequisites

* **JDK**: Version 21
* **Maven**: For building the project
* **Docker**: For Kafka, Zookeeper, Elasticsearch, and Kibana
* **IntelliJ IDEA**: For development and running the Flink job
* **Apache Flink**: Version 2.1.0 ([https://flink.apache.org/downloads/](https://flink.apache.org/downloads/))
* **Python**: For simulating streaming data to Kafka (`confluent-kafka` package)
* **Git**: For cloning and controlling versions of code

---

### Installation

```bash
# Clone repository
git clone https://github.com/TranNguyenPhucAnh/Apache.Flink.Streaming.For.Sales.Analytics.git
cd Apache.Flink.Streaming.For.Sales.Analytics

# Build project
mvn clean install
```

---

### Python Setup

```bash
pip3 install confluent-kafka
```

### Docker Setup

```bash
# Start services
cd scripts
docker-compose up -d

# Verify running containers
docker ps
```

### Flink Setup
```bash
# Navigate to the folder containing downloaded flink
`./flink-2.1.0/bin/start-cluster.sh`
```

**Services**:

* Kafka → `localhost:9092`
* Flink → `localhost:8081`
* Elasticsearch → `localhost:9200`
* Kibana → `localhost:5601`

---

### Create Kafka Topic

```bash
docker exec -it broker kafka-topics \
  --create --topic sales-topic \
  --bootstrap-server localhost:9092 \
  --partitions 1 --replication-factor 1
```

**Verify topic**:

```bash
docker exec -it broker kafka-topics \
  --bootstrap-server localhost:9092 \
  --list
```

---

## ▶️ Running the Application

### 1. Run Flink Job

* Open **`src/main/java/salesAnalysis/DataStreamJob.java`** in IntelliJ
* Run `flink run -c salesAnalysis.DataStreamJob target/SalesAnalysis-1.0-SNAPSHOT.jar`

---

### 2. Simulate Streaming Data

Run producer:

```bash
python3 scripts/send_to_kafka.py
```

Script (`send_to_kafka.py`):

```python
from confluent_kafka import Producer
import csv, time

producer = Producer({'bootstrap.servers': 'localhost:9092'})
with open('Datasets/order_items.csv', 'r') as file:
    reader = csv.reader(file)
    next(reader)  # skip header
    for row in reader:
        producer.produce('sales-topic', value=','.join(row).encode('utf-8'))
        producer.flush()
        time.sleep(1)  # simulate streaming
```

---

### 3. Verify Output

**Kafka**

   ```bash
   docker exec -it broker kafka-console-consumer \
     --bootstrap-server localhost:9092 \
     --topic sales-topic --from-beginning
   ```

**Flink UI: [http://localhost:8081](http://localhost:8081)**


**Elasticsearch**

* **Aggregated by category**

```bash
curl http://localhost:9200/category-sales/_search
```

* **Raw joined order items**

```bash
curl http://localhost:9200/order-items-raw/_search
```

---

### 4. Visualize in Kibana

1. Open Kibana → [http://localhost:5601](http://localhost:5601)
2. Create **index patterns**:
    * `category-sales`
    * `order-items-raw`
3. Build dashboards:

    * **Charts** for category-wise sales (`category-sales`)
    * **Charts** for product-level sales (`order-items-raw`)
4. Add to a dashboard → use **filters** for drill-down.

---

## 📂 Project Structure

```
Datasets/
 ├── order_items.csv   # Sales data (OrderItemID, OrderID, ProductID, Quantity, PricePerUnit)
 └── products.csv      # Product data (ProductID, Name, Description, Price, Category)

src/main/java/salesAnalysis/
 ├── entities/
 │   ├── OrderItem.java
 │   └── Product.java
 ├── dto/
 │   └── CategorySalesDTO.java
 ├── ProductBroadcastJoin.java   # Broadcast join logic
 └── DataStreamJob.java          # Main streaming pipeline

scripts/
 ├──send_to_kafka.py   # Python Kafka producer
 └── docker-compose.yml   # Docker

```

---

## 📦 Dependencies (pom.xml)

* `flink-streaming-java:2.1.0`
* `flink-connector-kafka:3.2.0-2.0`
* `flink-connector-elasticsearch7:4.0.0-2.0`
* `lombok:1.18.34`

---

## 🐳 Docker Compose Services

* **Zookeeper**: `confluentinc/cp-zookeeper:7.4.0`
* **Kafka**: `confluentinc/cp-kafka:7.4.0`
* **Elasticsearch**: `docker.elastic.co/elasticsearch/elasticsearch:7.17.0`
* **Kibana**: `docker.elastic.co/kibana/kibana:7.17.0`

---

## 🎥 Demo Instructions

1. Start Docker environment `docker-compose up -d`
2. Start Flink cluster `./flink-2.1.0/bin/start-cluster.sh`
3. Create Kafka topic

```bash
docker exec -it broker kafka-topics \
  --create --topic sales-topic \
  --bootstrap-server localhost:9092 \
  --partitions 1 --replication-factor 1
```

```bash
#Verify topic
docker exec -it broker kafka-topics \
  --bootstrap-server localhost:9092 \
  --list
```

4. Build jar `mvn install package`
5. Submit jar to Flink `flink run -c salesAnalysis.DataStreamJob target/SalesAnalysis-1.0-SNAPSHOT.jar`
6. Run producer script `./scripts/send_to_kafka.py`
7. Show Kafka consumer:

   ```bash
   docker exec -it broker kafka-console-consumer \
     --bootstrap-server localhost:9092 \
     --topic sales-topic --from-beginning
   ```
   
8. Open Flink UI → monitor job
9. Verify elasticsearch output
10. Open Kibana → visualize dashboards

---

## 📝 Notes (optional)

* Delete Elasticsearch indexes:

  ```bash
  curl -X DELETE http://localhost:9200/category-sales
  curl -X DELETE http://localhost:9200/order-items-raw
  ```
* Reset Kafka offset:

```bash
docker exec -it broker \
  kafka-consumer-groups --bootstrap-server localhost:9092 \
  --group flink-consumer-group \
  --reset-offsets --to-earliest --all-topics --execute
```

*  Delete Kafka topic:

```bash
docker exec -it broker kafka-topics \
  --delete --topic sales-topic --bootstrap-server localhost:9092
```

---