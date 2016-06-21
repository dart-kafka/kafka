# Running tests

Requirements:

* Docker Toolbox (OS X)

## Kafka 0.8.x

```
docker build -t kafka-cluster tool/kafka-cluster/
docker run -d --name kafka-cluster -p 2181:2181 -p 9092:9092 -p 9093:9093 --env ADVERTISED_HOST=192.168.99.100 kafka-cluster
```

Sometimes zookeeper needs a kick:

```
docker exec kafka-cluster bash -c '$KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper=localhost:2181 --topic dartKafkaTest --partitions 3 --replication-factor 2'
```

Now you should be able to run tests with:

```
pub run test -j 1
```

## Kafka 0.10.0.0

We're using `ches/kafka` base image so instructions are a bit different:

```
# Zookeeper is in a separate container now
docker run -d --name zookeeper --publish 2181:2181 jplock/zookeeper:3.4.6

# Build our image
docker build -t kafka tool/kafka-0.10.0.0/

# Start first Kafka broker in it's own container
docker run -d \
    --hostname 192.168.99.100 \
    --name kafka1 \
    --publish 9092:9092 \
    --env KAFKA_ADVERTISED_HOST_NAME=192.168.99.100 --env ZOOKEEPER_IP=192.168.99.100 \
    --env KAFKA_BROKER_ID=1 \
    kafka

# Start second Kafka broker in it's own container
docker run -d \
    --hostname 192.168.99.100 \
    --name kafka2 \
    --publish 9093:9093 \
    --env KAFKA_ADVERTISED_HOST_NAME=192.168.99.100 --env ZOOKEEPER_IP=192.168.99.100 \
    --env KAFKA_PORT=9093 --env KAFKA_ADVERTISED_PORT=9093 --env KAFKA_BROKER_ID=2 \
    kafka
```

Optionally, can force it to create a topic

```
docker run --rm kafka \
  kafka-topics.sh --create --topic bootstrapTopic --replication-factor 2 --partitions 3 --zookeeper 192.168.99.100:2181
```
