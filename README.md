# Dart Kafka

[![Build Status](https://img.shields.io/travis-ci/pulyaevskiy/dart-kafka.svg?branch=master&style=flat-square)](https://travis-ci.org/pulyaevskiy/dart-kafka)
[![Coverage Status](https://img.shields.io/coveralls/pulyaevskiy/dart-kafka.svg?branch=master&style=flat-square)](https://coveralls.io/github/pulyaevskiy/dart-kafka?branch=master)
[![License](https://img.shields.io/badge/license-BSD--2-blue.svg?style=flat-square)](https://raw.githubusercontent.com/pulyaevskiy/dart-kafka/master/LICENSE)

Kafka client library written in Dart.

### Current status

This library is a work-in-progress and has not been used in production yet.

### Things that are not supported yet.

* Snappy compression.

## Installation

There is no Pub package yet, but it will be published as soon as APIs are
stable enough.

For now you can use git dependency in your `pubspec.yaml`:

```yaml
dependencies:
  kafka:
    git: https://github.com/pulyaevskiy/dart-kafka.git
```

And then import it as usual:

```dart
import 'package:kafka/kafka.dart';
```

## Features

This library provides several high-level API objects to interact with Kafka:

* __KafkaSession__ - responsible for managing connections to Kafka brokers and
  coordinating all requests. Also provides access to metadata information.
* __Producer__ - publishes messages to Kafka topics
* __Consumer__ - consumes messages from Kafka topics and stores it's state (current
  offsets). Leverages ConsumerMetadata API via ConsumerGroup.
* __Fetcher__ - consumes messages from Kafka without storing state.
* __OffsetMaster__ - provides convenience on top of Offset API allowing to easily
  retrieve earliest and latest offsets of particular topic-partitions.
* __ConsumerGroup__ - provides convenience on top of Consumer Metadata API to easily
  fetch or commit consumer offsets.

## Producer

Simple implementation of Kafka producer. Supports auto-detection of leaders for
topic-partitions and creates separate `ProduceRequest`s for each broker.
Requests are sent in parallel and all responses are aggregated in special
`ProduceResult` object.

```dart
// file:produce.dart
import 'dart:io';
import 'package:kafka/kafka.dart';

main(List<String> arguments) async {
  var host = new ContactPoint('127.0.0.1', 9092);
  var session = new KafkaSession([host]);

  var producer = new Producer(session, 1, 1000);
  var result = await producer.produce([
    new ProduceEnvelope('topicName', 0, [new Message('msgForPartition0'.codeUnits)]),
    new ProduceEnvelope('topicName', 1, [new Message('msgForPartition1'.codeUnits)])
  ]);
  print(result.hasErrors);
  print(result.offsets);
  session.close(); // make sure to always close the session when the work is done.
}
```

Result:

```shell
$ dart produce.dart
$ false
$ {dartKafkaTest: {0: 213075, 1: 201680}}
```

## Consumer

High-level implementation of Kafka consumer which stores it's state using
Kafka's ConsumerMetadata API.

> If you don't want to keep state of consumed offsets take a look at `Fetcher`
> which was designed specifically for this use case.

Consumer returns messages as a stream so all standard stream operations
should be applicable. However there is few important things to know about how
Consumer works.

1. All messages are returned in special `MessageEnvelope` object which provides
  some additional information about the message: topicName, partitionId and the
  offset.
2. When consuming messages you must signal to the Consumer when to proceed to
  the next message and whether to commit the offset of the current message. This
  is possible via two methods provided by `MessageEnvelope`: `commit()` and
  `ack()`. The `ack()` method only tells to Consumer that we are ready for the
  next message, therefore offset of the current message will not be committed.
  The `commit()` method will also tell to Consumer to commit current offset.
3. There is also `cancel()` method on `MessageEnvelope` which signals to the
  Consumer to stop the process. No more messages will be added after you call
  `cancel()` and the consumer stream will be closed.

Simplest example of a consumer:

```dart
import 'dart:io';
import 'dart:async';
import 'package:kafka/kafka.dart';

void main(List<String> arguments) async {
  var host = new ContactPoint('127.0.0.1', 9092);
  var session = new KafkaSession([host]);
  var group = new ConsumerGroup(session, 'consumerGroupName');
  var topics = {
    'topicName': [0, 1] // list of partitions to consume from.
  };

  var consumer = new Consumer(session, group, topics, 100, 1);
  await for (MessageEnvelope envelope in consumer.consume()) {
    // Assuming that messages were produces by Producer from previous example.
    var value = new String.fromCharCodes(envelope.message.value);
    print('Got message: ${envelope.offset}, ${value}');
    envelope.commit('metadata');
  }
  session.close(); // make sure to always close the session when the work is done.
}
```

It is also possible to consume messages in batches for improved efficiency:

```dart
import 'dart:io';
import 'dart:async';
import 'package:kafka/kafka.dart';

void main(List<String> arguments) async {
  var host = new ContactPoint('127.0.0.1', 9092);
  var session = new KafkaSession([host]);
  var group = new ConsumerGroup(session, 'consumerGroupName');
  var topics = {
    'topicName': [0, 1] // list of partitions to consume from.
  };

  var consumer = new Consumer(session, group, topics, 100, 1);
  await for (BatchEnvelope batch in consumer.batchConsume(20)) {
    batch.items.forEach((MessageEnvelope envelope) {
      // use envelope as usual
    });
    batch.commit('metadata'); // use batch control methods instead of individual messages.
  }
  session.close(); // make sure to always close the session when the work is done.
}
```


### Supported protocol versions

Current version targets version `0.8.2` of the Kafka protocol. There is no plans
to support earlier versions.

### License

BSD-2
