#!/bin/bash

docker kill kafka zookeeper
docker rm kafka zookeeper

docker run -d --name zookeeper --publish 2181:2181 zookeeper:3.6.0

docker build -t kafka tool/kafka/

ZK_IP=$(docker inspect --format '{{ .NetworkSettings.IPAddress }}' zookeeper)

docker run -d --name kafka --publish 9092:9092 --publish 9093:9093 \
  --env KAFKA_ADVERTISED_HOST_NAME=127.0.0.1 \
  --env ZOOKEEPER_IP=$ZK_IP \
  kafka

sleep 5