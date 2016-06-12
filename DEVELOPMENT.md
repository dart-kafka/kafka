# Running tests

Requirements:

* Docker Toolbox (OS X)

## Starting Kafka container locally

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
