# Taxi Trip Streaming Analytics


## Question 1: Find the version of redpandas in the docker container.
```bash
docker exec -it redpanda-1 rpk version
```

**Output:**
Version:     v24.2.18
Git ref:     f9a22d4430
Build date:  2025-02-14T12:52:55Z
OS/Arch:     linux/amd64
Go version:  go1.23.1

Redpanda Cluster
  node-1  v24.2.18 - f9a22d443087b824803638623d6b7492ec8221f9

**Alternative method - run in shell, then find version (same result)**
```bash
docker exec -it redpanda-1 /bin/sh
rpk version
```


## Question 2. Creating a topic - What's the output of the command for creating a topic? Include the entire output in your answer.

**Command**
```bash
docker exec -it redpanda-1 rpk topic create green-trips
```

**Answer / Output:**
```bash
TOPIC        STATUS
green-trips  OK
```

## Question 3. Connecting to the Kafka server - what's the output?

```python
import json

from kafka import KafkaProducer

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

server = 'localhost:9092'

producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=json_serializer
)

producer.bootstrap_connected()
```

**Answer: True**





## Question 4: Sending the Trip Data - How much time did it take to send the entire dataset and flush? 

**Answer: Time taken to send and flush data: 41.758378982543945 seconds**




## Question 5: Build a Sessionization Window (2 points) - Which pickup and drop off locations have the longest unbroken streak of taxi trips?

**Answer**

Pick up loc - 22 (Brooklyn, Bensonhurst West); drop off loc - 22 --> 6 trips

Pick up loc - 129 (Queens, Jackson Heights) drop off loc - 129 --> 6 trips
