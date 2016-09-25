# Dart Kafka

[![Build Status](https://travis-ci.org/pulyaevskiy/dart-kafka.svg?branch=master)](https://travis-ci.org/pulyaevskiy/dart-kafka)
[![Coverage](https://codecov.io/gh/pulyaevskiy/dart-kafka/branch/master/graph/badge.svg)](https://codecov.io/gh/pulyaevskiy/dart-kafka)
[![License](https://img.shields.io/badge/license-BSD--2-blue.svg)](https://raw.githubusercontent.com/pulyaevskiy/dart-kafka/master/LICENSE)

Kafka client library written in Dart.

### Current status

This library is a work-in-progress.
Currently all the updates are happening in `kafka-0.10` branch:

* Support for Kafka 0.10 APIs including Group Membership API
* Implementation of `HighLevelConsumer` capable of automatic load-balancing
  and re-distribution of topics/partitions in case of failures.
* Better testing framework.
* Isolate-based distribution of consumer group members for better utilization
  of system resources.

Master branch currently targets 0.8.x versions of Kafka server.

### Things that are not supported yet.

* Snappy compression.

## Installation

There is no Pub package yet, but it will be published as soon as APIs are
stable enough.

For now you can use git dependency in your `pubspec.yaml`:

```yaml
dependencies:
  kafka:
    git: https://github.com/dart-drivers/kafka.git
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

Consumer returns messages as a `Stream`, so all standard stream operations
should be applicable. However Kafka topics are ordered streams of messages
with sequential offsets. Consumer implementation allows to preserve order of
messages received from server. For this purpose all messages are wrapped in
special `MessageEnvelope` object with following methods:

```
/// Signals to consumer that message has been processed and it's offset can
/// be committed.
void commit(String metadata);

/// Signals that message has been processed and we are ready for
/// the next one. Offset of this message will **not** be committed.
void ack();

/// Signals to consumer to cancel any further deliveries and close the stream.
void cancel();
```

One must call `commit()` or `ack()` for each processed message, otherwise
Consumer won't send the next message to the stream.

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
  await for (MessageEnvelope envelope in consumer.consume(limit: 3)) {
    // Assuming that messages were produces by Producer from previous example.
    var value = new String.fromCharCodes(envelope.message.value);
    print('Got message: ${envelope.offset}, ${value}');
    envelope.commit('metadata'); // Important.
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

### Consumer offset reset strategy

Due to the fact that Kafka topics can be configured to delete old messages
periodically, it is possible that your consumer offset may become invalid (
just because there is no such message/offset in Kafka topic anymore).

In such cases `Consumer` provides configurable strategy with following options:

* `OffsetOutOfRangeBehavior.throwError`
* `OffsetOutOfRangeBehavior.resetToEarliest` (default)
* `OffsetOutOfRangeBehavior.resetToLatest`

By default if it gets `OffsetOutOfRange` server error it will reset it's offsets
to earliest available in the consumed topic and partitions, which essentially
means consuming all available messages from the beginning.

To modify this behavior simply set `onOffsetOutOfRange` property of consumer to
one of the above values:

```
var consumer = new Consumer(session, group, topics, 100, 1);
consumer.onOffsetOutOfRange = OffsetOutOfRangeBehavior.throwError;
```

## Supported protocol versions

Current version targets version `0.8.2` of the Kafka protocol. There is no plans
to support earlier versions.

## License

BSD-2
